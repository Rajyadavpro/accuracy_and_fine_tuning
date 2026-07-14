import os
import json
import logging
import datetime
import pymysql
from pymysql.cursors import DictCursor
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from langfuse import Langfuse

# Import your existing DB resolver
from accuracy.healthcare_accuracy import resolve_db_config, resolve_table_name

SERVICE_BUS_CONN_STR = os.getenv("SERVICE_BUS_CONNECTION_STRING")
SB_QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME", "accuracy-queue")
LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT", "DEV").strip()

# ---------------------------------------------------------
# LANGFUSE CHECKPOINT HELPERS
# ---------------------------------------------------------
def _get_langfuse_client() -> Langfuse | None:
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
    
    if not public_key or not secret_key:
        logging.warning("Missing Langfuse credentials. Checkpoint will not be saved.")
        return None
    return Langfuse(public_key=public_key.strip(), secret_key=secret_key.strip(), host=host.strip())

def get_checkpoint_dataset_name(file_type: str) -> str:
    # Ensures "accuracy" is in the dataset name as requested
    return f"{file_type.lower()}_accuracy_queue_checkpoint_{LANGFUSE_ENVIRONMENT}"

def get_last_processed_id(file_type: str) -> int:
    langfuse = _get_langfuse_client()
    if not langfuse:
        return 0
        
    dataset_name = get_checkpoint_dataset_name(file_type)
    max_id = 0
    try:
        dataset = langfuse.get_dataset(dataset_name)
        for item in dataset.items:
            payload = item.input if isinstance(item.input, dict) else {}
            val = payload.get("last_processed_id")
            if val:
                try:
                    val_int = int(val)
                    if val_int > max_id:
                        max_id = val_int
                except ValueError:
                    continue
    except Exception:
        # Dataset likely doesn't exist yet
        pass
        
    return max_id

def save_last_processed_id(file_type: str, last_id: int) -> None:
    langfuse = _get_langfuse_client()
    if not langfuse:
        return
        
    dataset_name = get_checkpoint_dataset_name(file_type)
    
    try:
        langfuse.create_dataset(
            name=dataset_name,
            description=f"Checkpoint tracker for {file_type} accuracy Service Bus queue."
        )
    except Exception:
        pass # Dataset already exists
        
    timestamp = datetime.datetime.utcnow().isoformat()
    try:
        langfuse.create_dataset_item(
            dataset_name=dataset_name,
            id=f"checkpoint::{timestamp}",
            input={
                "last_processed_id": last_id, 
                "file_type": file_type, 
                "timestamp": timestamp
            }
        )
        langfuse.flush()
        logging.info(f"[{file_type}] Langfuse checkpoint successfully updated to Id: {last_id}")
    except Exception as e:
        logging.error(f"[{file_type}] Failed to save Langfuse checkpoint: {e}")

# ---------------------------------------------------------
# MAIN DISPATCHER
# ---------------------------------------------------------
def main(ids_per_message: int | None = None, max_messages_per_run: int | None = None) -> None:
    file_type = "EOB"
    logging.info(f"[{file_type}] Starting incremental Service Bus queue dispatch...")
    
    if not SERVICE_BUS_CONN_STR:
        logging.error(f"[{file_type}] SERVICE_BUS_CONNECTION_STRING is missing.")
        return

    # 1. Fetch Checkpoint from Langfuse
    last_processed_id = get_last_processed_id(file_type)
    logging.info(f"[{file_type}] Fetching records incrementally after Id: {last_processed_id}")

    cfg = resolve_db_config()
    conn = pymysql.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, 
        password=cfg.password, database=cfg.database, cursorclass=DictCursor
    )
    
    with conn:
        tables = {"allocation": resolve_table_name(conn, ["EOB_Allocation", "EOBAllocations"])}
        
        # 2. Optimized SQL Query (Only fetches IDs greater than the checkpoint)
        with conn.cursor() as cur:
            query = f"""
                SELECT Id FROM {tables['allocation']} 
                WHERE rawJson IS NOT NULL AND rawJson <> '' 
                AND Id > %s 
                ORDER BY Id ASC
            """
            cur.execute(query, (last_processed_id,))
            fetched_rows = cur.fetchall()
        
        fetched_ids = [row["Id"] for row in fetched_rows if row.get("Id")]

    if not fetched_ids:
        logging.info(f"[{file_type}] No new eligible IDs found since last checkpoint.")
        return

    # 3. Chunk the IDs based on ids_per_message
    chunk_size = ids_per_message if ids_per_message and ids_per_message > 0 else 1
    chunks = [fetched_ids[i:i + chunk_size] for i in range(0, len(fetched_ids), chunk_size)]
    
    # 4. Enforce max_messages_per_run limit
    if max_messages_per_run is not None and max_messages_per_run > 0:
        chunks = chunks[:max_messages_per_run]

    highest_id_sent = last_processed_id
    
    # 5. Bulk-send Service Bus messages
    with ServiceBusClient.from_connection_string(SERVICE_BUS_CONN_STR) as sb_client:
        with sb_client.get_queue_sender(queue_name=SB_QUEUE_NAME) as sb_sender:
            messages_to_send = []
            
            for chunk in chunks:
                payload = {
                    "id": chunk,  # Array of IDs
                    "process_type": "Accuracy",
                    "file_type": file_type
                }
                messages_to_send.append(ServiceBusMessage(json.dumps(payload)))
                
                # Keep track of the highest ID in this run
                highest_id_sent = max(highest_id_sent, max(chunk))
            
            if messages_to_send:
                sb_sender.send_messages(messages_to_send)
                logging.info(f"[{file_type}] Successfully bulk-sent {len(messages_to_send)} messages.")
                
                # 6. Save the new Checkpoint to Langfuse ONLY after successful queue dispatch
                save_last_processed_id(file_type, highest_id_sent)

if __name__ == "__main__":
    # Example test run triggers
    main(ids_per_message=10, max_messages_per_run=5)