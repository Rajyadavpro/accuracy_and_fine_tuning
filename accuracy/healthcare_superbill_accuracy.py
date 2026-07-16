import os
import json
import logging
import datetime
import pymysql
from pymysql.cursors import DictCursor
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import requests

# Import your existing DB resolver
from accuracy.healthcare_accuracy import resolve_db_config, resolve_table_name
from clickhouse_store import _get_clickhouse_config, get_environment

SERVICE_BUS_CONN_STR = os.getenv("SERVICE_BUS_CONNECTION_STRING")
SB_QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME", "accuracy-queue")
CLICKHOUSE_ENVIRONMENT = get_environment()
SUPERBILL_CHECKPOINT_TABLE = "superbill_accuracy_checkpoint"

# ---------------------------------------------------------
# CLICKHOUSE CHECKPOINT HELPERS
# ---------------------------------------------------------
def ensure_superbill_checkpoint_table(timeout: int = 10) -> bool:
    """Create Superbill checkpoint table in ClickHouse if it doesn't exist."""
    host, http_port, database, user, password = _get_clickhouse_config()
    url = f"http://{host}:{http_port}/"
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{database}`.`{SUPERBILL_CHECKPOINT_TABLE}` (
        environment String,
        file_type String,
        last_processed_id UInt64,
        updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (environment, file_type)
    """
    
    try:
        response = requests.post(url, auth=(user, password), data=ddl.encode(), timeout=timeout)
        if response.status_code in (200, 201):
            logging.info(f"[ClickHouse] Superbill checkpoint table ready")
            return True
        else:
            logging.error(f"[ClickHouse] Failed to create checkpoint table: {response.status_code} - {response.text[:200]}")
            return False
    except Exception as e:
        logging.error(f"[ClickHouse] Error ensuring checkpoint table: {e}")
        return False

def get_last_processed_id(file_type: str) -> int:
    """Fetch the last processed ID from ClickHouse checkpoint table."""
    host, http_port, database, user, password = _get_clickhouse_config()
    url = f"http://{host}:{http_port}/"
    
    query = f"""
    SELECT max(last_processed_id) as max_id FROM `{database}`.`{SUPERBILL_CHECKPOINT_TABLE}` 
    WHERE environment = '{CLICKHOUSE_ENVIRONMENT}' AND file_type = '{file_type}'
    """
    
    try:
        response = requests.get(url, auth=(user, password), params={"query": query}, timeout=10)
        if response.status_code == 200:
            result = response.text.strip()
            if result and result != "0" and result != "\\N":
                return int(result)
    except Exception as e:
        logging.warning(f"[ClickHouse] Error loading checkpoint: {e}")
    
    return 0

def save_last_processed_id(file_type: str, last_id: int) -> None:
    """Save the last processed ID to ClickHouse checkpoint table."""
    host, http_port, database, user, password = _get_clickhouse_config()
    url = f"http://{host}:{http_port}/"
    
    insert_sql = f"""
    INSERT INTO `{database}`.`{SUPERBILL_CHECKPOINT_TABLE}` (environment, file_type, last_processed_id) 
    VALUES ('{CLICKHOUSE_ENVIRONMENT}', '{file_type}', {last_id})
    """
    
    try:
        response = requests.post(url, auth=(user, password), data=insert_sql.encode(), timeout=30)
        if response.status_code in (200, 201):
            logging.info(f"[ClickHouse] Checkpoint saved: {file_type} -> Id {last_id}")
        else:
            logging.error(f"[ClickHouse] Failed to save checkpoint: {response.status_code} - {response.text[:200]}")
    except Exception as e:
        logging.error(f"[ClickHouse] Error saving checkpoint: {e}")

# ---------------------------------------------------------
# MAIN DISPATCHER
# ---------------------------------------------------------
def main(ids_per_message: int | None = None, max_messages_per_run: int | None = None) -> None:
    file_type = "Superbill"
    logging.info(f"[{file_type}] Starting incremental Service Bus queue dispatch...")
    
    # Ensure checkpoint table exists
    if not ensure_superbill_checkpoint_table():
        logging.error(f"[{file_type}] Failed to initialize ClickHouse checkpoint table")
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
        tables = {"allocation": resolve_table_name(conn, ["AllocationRecord", "SuperBillAllocation", "SuperBillAllocations"])}
        
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
                    "file_type": "Superbill"
                }
                messages_to_send.append(ServiceBusMessage(json.dumps(payload)))
                
                # Keep track of the highest ID in this run
                highest_id_sent = max(highest_id_sent, max(chunk))
            
            if messages_to_send:
                sb_sender.send_messages(messages_to_send)
                logging.info(f"[Superbill] Successfully bulk-sent {len(messages_to_send)} messages.")
                
                # 6. Save the new Checkpoint to Langfuse ONLY after successful queue dispatch
                save_last_processed_id(file_type, highest_id_sent)

if __name__ == "__main__":
    # Example test run triggers
    main(ids_per_message=10, max_messages_per_run=5)