# import os
# import pymysql
# import logging
# import json
# import tempfile
# from pathlib import Path
# from datetime import datetime, timezone
# from typing import Optional, List
# from azure.servicebus import ServiceBusClient, ServiceBusMessage

# # ==========================================
# # SETUP DUAL-LOGGING (CONSOLE & FILE)
# # ==========================================

# DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "idp_fine_tuning_data_push.log")
# LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", DEFAULT_LOG_FILE)
# LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
# LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

# root_logger = logging.getLogger()
# root_logger.setLevel(LOG_LEVEL)

# for handler in root_logger.handlers[:]:
#     root_logger.removeHandler(handler)

# log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s")

# console_handler = logging.StreamHandler()
# console_handler.setLevel(LOG_LEVEL)
# console_handler.setFormatter(log_formatter)
# root_logger.addHandler(console_handler)

# file_write_success = False
# try:
#     file_handler = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
#     file_handler.setLevel(LOG_LEVEL)
#     file_handler.setFormatter(log_formatter)
#     root_logger.addHandler(file_handler)
#     file_write_success = True
# except Exception as log_ex:
#     logging.error(f"Failed to initialize log file at '{LOG_FILE_PATH}': {log_ex}")

# logging.info("=============================================================")
# logging.info(f"LOGGING INITIALIZED. Level: {logging.getLevelName(LOG_LEVEL)}")
# if file_write_success:
#     logging.info(f"Log file: {LOG_FILE_PATH}")
# logging.info("=============================================================")

# try:
#     from langfuse import Langfuse
#     logging.info("[Langfuse] Library imported successfully.")
# except ImportError:
#     logging.error("[Langfuse] FAILURE: Langfuse library not installed.")
#     Langfuse = None


# def _mask_value(val: Optional[str]) -> str:
#     """Masks secret values in logs."""
#     if not val:
#         return "Not Set"
#     val = val.strip()
#     if len(val) <= 4:
#         return "****"
#     return f"{val[:2]}...{val[-2:]}"


# def _extract_filename(path_value: str) -> str:
#     """Extracts raw filename from a remote URL path."""
#     val = str(path_value).strip() if path_value is not None else ""
#     if val:
#         name = Path(val).name
#         if name:
#             return name
#     return "unknown_file.pdf"


# # ==========================================
# # LANGFUSE CLIENT & CHECKPOINT UTILITIES
# # ==========================================

# def _get_langfuse_client():
#     """Initialize Langfuse client."""
#     logging.info("[Langfuse] Initializing client...")
    
#     if not Langfuse:
#         raise ImportError("Langfuse library is not installed.")

#     public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
#     secret_key = os.getenv("LANGFUSE_SECRET_KEY")
#     host = os.getenv("LANGFUSE_HOST")

#     logging.info(f"[Langfuse] Config: PK={_mask_value(public_key)}, SK={_mask_value(secret_key)}, Host={host}")

#     if not all([public_key, secret_key, host]):
#         raise ValueError("Missing Langfuse credentials: LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY, LANGFUSE_HOST")

#     try:
#         client = Langfuse(public_key=public_key.strip(), secret_key=secret_key.strip(), host=host.strip())
#         logging.info("[Langfuse] SUCCESS: Client initialized.")
#         return client
#     except Exception as e:
#         logging.error(f"[Langfuse] FAILURE: {e}", exc_info=True)
#         raise e


# def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[int]:
#     """Retrieve last processed ID from Langfuse."""
#     logging.info(f"[Langfuse] Loading checkpoint from dataset '{checkpoint_dataset_name}'...")
#     try:
#         langfuse = _get_langfuse_client()
#     except Exception as init_err:
#         logging.warning(f"[Langfuse] Could not initialize client: {init_err}. Starting clean.")
#         return None

#     try:
#         dataset = langfuse.get_dataset(checkpoint_dataset_name)
#         if not dataset or not hasattr(dataset, 'items') or not dataset.items:
#             logging.info(f"[Langfuse] Dataset empty or not found. Clean start.")
#             return None
        
#         latest_item = max(dataset.items, key=lambda r: getattr(r, 'created_at'))
#         checkpoint_data = latest_item.input
        
#         if isinstance(checkpoint_data, dict) and "last_id" in checkpoint_data:
#             last_id = int(checkpoint_data["last_id"])
#             logging.info(f"[Langfuse] SUCCESS: Retrieved checkpoint last_id='{last_id}'")
#             return last_id
#         else:
#             logging.error(f"[Langfuse] Checkpoint payload invalid: {checkpoint_data}")
#     except Exception as ex:
#         logging.warning(f"[Langfuse] Could not retrieve checkpoint: {ex}")
    
#     return None


# def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, last_id: int) -> None:
#     """Save last processed ID to Langfuse."""
#     logging.info(f"[Langfuse] Saving checkpoint: last_id='{last_id}'...")
#     try:
#         langfuse = _get_langfuse_client()
#     except Exception as init_err:
#         logging.error(f"[Langfuse] Cannot save checkpoint: {init_err}")
#         raise init_err

#     try:
#         langfuse.create_dataset(
#             name=checkpoint_dataset_name,
#             description=f"IDP Fine Tuning checkpoint ({langfuse_environment})"
#         )
        
#         checkpoint_item_id = f"checkpoint::id::{last_id}"
#         checkpoint_payload = {
#             "last_id": last_id,
#             "saved_at": datetime.now(timezone.utc).isoformat(),
#         }
        
#         langfuse.create_dataset_item(
#             dataset_name=checkpoint_dataset_name,
#             id=checkpoint_item_id,
#             input=checkpoint_payload,
#             metadata={"record_type": "idp_finetuning_checkpoint", "last_id": last_id}
#         )
        
#         langfuse.flush()
#         logging.info(f"[Langfuse] SUCCESS: Checkpoint saved with last_id='{last_id}'")
#     except Exception as ex:
#         logging.error(f"[Langfuse] FAILURE: {ex}", exc_info=True)
#         raise ex


# # ==========================================
# # DATABASE UTILITIES
# # ==========================================

# def _get_db_connection(server: str, port: str, database: str, user: str, password: str):
#     """Create MySQL/MariaDB connection."""
#     logging.info(f"[Database] Connecting to {server}:{port}/{database}...")
    
#     if not all([server, database, user, password]):
#         raise ValueError("Missing database credentials: IDP_DB_SERVER, IDP_DB_DATABASE, IDP_DB_USERID, IDP_DB_PASSWORD")

#     try:
#         connection = pymysql.connect(
#             host=server,
#             port=int(port),
#             user=user,
#             password=password,
#             database=database,
#             connect_timeout=30,
#             charset="utf8mb4"
#         )
#         logging.info("[Database] SUCCESS: Connected.")
#         return connection
#     except Exception as e:
#         logging.error(f"[Database] FAILURE: {e}", exc_info=True)
#         raise e


# # ==========================================
# # QUEUE UTILITIES
# # ==========================================

# def _send_to_azure_queue(queue_name: str, messages: List[str]) -> None:
#     """Dispatch messages to Service Bus queue."""
#     logging.info(f"[Queue] Dispatching {len(messages)} messages to '{queue_name}'...")
#     connection_string = os.getenv("SERVICE_BUS_CONNECTION_STRING")
#     if not connection_string:
#         raise ValueError("SERVICE_BUS_CONNECTION_STRING is missing.")

#     try:
#         with ServiceBusClient.from_connection_string(connection_string) as client:
#             with client.get_queue_sender(queue_name=queue_name) as sender:
#                 for idx, msg in enumerate(messages, 1):
#                     sender.send_messages(ServiceBusMessage(msg))
#                     logging.info(f"[Queue] Message {idx}/{len(messages)} sent.")
#     except Exception as e:
#         logging.error(f"[Queue] FAILURE: {e}", exc_info=True)
#         raise e
#     logging.info(f"[Queue] SUCCESS: All {len(messages)} messages sent.")


# # ==========================================
# # MAIN FINE TUNING TASK FUNCTION
# # ==========================================

# def idp_fine_tuning_data_push(
#     ids_per_message: Optional[int] = None, 
#     max_messages_per_run: Optional[int] = None
# ) -> None:
#     """Fetch metadata from IDP database and dispatch FineTuning message to Service Bus."""
    
#     logging.info("=============================================================")
#     logging.info("[Fine Tuning Task] Starting IDP Fine Tuning Data Push...")
#     logging.info("=============================================================")
    
#     # ==========================================
#     # RUNTIME ENVIRONMENT VALIDATION
#     # ==========================================
#     logging.info("[Config] Validating environment...")
    
#     langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "exp").strip()
#     checkpoint_dataset_name = f"idp_finetuning_checkpoint_{langfuse_environment}"
    
#     queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
#     if not queue_name:
#         raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")
    
#     ids_per_message_str = os.getenv("IDS_PER_MESSAGE", "10").strip()
#     eff_ids_per_message = ids_per_message if ids_per_message is not None else int(ids_per_message_str)
    
#     max_messages_per_run_str = os.getenv("MAX_MESSAGES_PER_RUN", "").strip()
#     eff_max_messages_per_run = max_messages_per_run if max_messages_per_run is not None else (int(max_messages_per_run_str) if max_messages_per_run_str else None)
    
#     logging.info(f"[Config] Environment valid. IDs/msg={eff_ids_per_message}, MaxMsgs={eff_max_messages_per_run}")
    
#     # ==========================================
#     # STEP 1: Load checkpoint
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 1: Loading checkpoint...")
#     last_checkpoint_id = _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
#     # ==========================================
#     # STEP 2: Build query
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 2: Building query...")
    
#     # TODO: Update table name and column logic to match your specific IDP SQL structure
#     query_params = []
#     where_clause = "rawJson IS NOT NULL AND rawJson != ''"
    
#     if last_checkpoint_id is not None:
#         logging.info(f"[Fine Tuning Task] Incremental: last_checkpoint_id='{last_checkpoint_id}'")
#         where_clause += " AND Id > %s"
#         query_params.append(last_checkpoint_id)
#     else:
#         logging.info("[Fine Tuning Task] Full dataset query (no checkpoint)")
    
#     query = (
#         "SELECT "
#         "  Id, "
#         "  File_name, "
#         "  File_url, "
#         "  rawJson "
#         "FROM `dbo.vw_PdfClassificationTransactionLog` " 
#         f"WHERE {where_clause} "
#         "ORDER BY Id ASC;"
#     )
    
#     # ==========================================
#     # STEP 3: Fetch from database
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 3: Executing database query...")
#     server = os.getenv("IDP_SQL_SERVER")
#     port = os.getenv("IDP_SQL_PORT", "3306")
#     database = os.getenv("IDP_SQL_DATABASE")
#     user = os.getenv("IDP_SQL_USER")
#     password = os.getenv("IDP_SQL_PASSWORD")
    
#     try:
#         with _get_db_connection(server, port, database, user, password) as conn:
#             cursor = conn.cursor()
#             cursor.execute(query, query_params)
#             rows = cursor.fetchall()
#     except Exception as e:
#         logging.error(f"[Fine Tuning Task] FAILURE: Database query failed: {e}", exc_info=True)
#         raise e
    
#     # ==========================================
#     # STEP 4: Check if empty
#     # ==========================================
#     if len(rows) == 0:
#         logging.info("[Fine Tuning Task] No new IDP records. Done.")
#         return
    
#     # ==========================================
#     # STEP 4.5: Deduplicate and parse records
#     # ==========================================
#     processed_records = []
#     seen_ids = set()
#     skipped = 0

#     for row in rows:
#         record_id = row[0]
#         if record_id is None or record_id in seen_ids:
#             continue
#         seen_ids.add(record_id)
        
#         file_name = _extract_filename(row[1] or row[2])
#         raw_json_str = row[3]

#         try:
#             parsed_ground_truth = json.loads(raw_json_str)
#         except json.JSONDecodeError as ex:
#             logging.info(f"  SKIP AllocationId={record_id}: rawJson parse error – {ex}")
#             skipped += 1
#             continue
            
#         client_code = None

#         # Read client identifier from parsed root first
#         if isinstance(parsed_ground_truth, dict):
#             client_code = parsed_ground_truth.get("client_code") or parsed_ground_truth.get("clientCode")

#         # Direct targeted match inside the nested "json" array
#         if isinstance(parsed_ground_truth, dict) and isinstance(parsed_ground_truth.get("json"), list) and parsed_ground_truth["json"]:
#             first_item = parsed_ground_truth["json"][0]
#             if isinstance(first_item, dict):
#                 client_code = client_code or (
#                     first_item.get("Client Code")
#                     or first_item.get("Client Name")
#                     or first_item.get("Client")
#                     or first_item.get("ClientName")
#                     or first_item.get("client_name")
#                     or first_item.get("client_code")
#                     or first_item.get("clientCode")
#                 )

#         # Fallback if fields are missing
#         client_code = client_code or "Unknown"
        
#         processed_records.append({
#             "allocation_id": record_id,
#             "file_name": file_name,
#             "client_code": client_code,
#             "ground_truth": parsed_ground_truth
#         })

#     logging.info(f"[Fine Tuning Task] Parsed {len(processed_records)} valid records. Skipped: {skipped}")
    
#     # ==========================================
#     # STEP 5: Batch IDs
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 5: Batching records...")
#     chunks = [processed_records[i:i + eff_ids_per_message] for i in range(0, len(processed_records), eff_ids_per_message)]
    
#     is_capped = False
#     if eff_max_messages_per_run is not None and len(chunks) > eff_max_messages_per_run:
#         logging.warning(f"[Fine Tuning Task] CAPPING: {len(chunks)} chunks > {eff_max_messages_per_run} max")
#         chunks = chunks[:eff_max_messages_per_run]
#         is_capped = True
    
#     logging.info(f"[Fine Tuning Task] Batched into {len(chunks)} message(s).")
    
#     # ==========================================
#     # STEP 6: Format messages with Service Bus schema
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 6: Formatting messages...")
#     formatted_messages = []
#     for chunk in chunks:
#         allocation_ids = [r["allocation_id"] for r in chunk]
        
#         payload = {
#             "file_name": chunk[0].get("file_name") if len(chunk) == 1 else None,
#             "allocation_id": chunk[0].get("allocation_id") if len(chunk) == 1 else None,
#             "client_code": chunk[0].get("client_code") if len(chunk) == 1 else None,
#             "ground_truth": chunk[0].get("ground_truth") if len(chunk) == 1 else None,
#             "allocation_ids": allocation_ids,
#             "records": chunk,
#             "source": "idp",
#             "environment": langfuse_environment,
#             "process_type": "FineTuning",
#             "queued_at": datetime.now(timezone.utc).isoformat()
#         }
#         formatted_messages.append(json.dumps(payload))
    
#     last_dispatched_id = chunks[-1][-1]["allocation_id"]
#     logging.info(f"[Fine Tuning Task] Last ID: '{last_dispatched_id}'")
    
#     # ==========================================
#     # STEP 7: Dispatch to queue
#     # ==========================================
#     logging.info(f"[Fine Tuning Task] Step 7: Dispatching {len(formatted_messages)} message(s)...")
#     try:
#         _send_to_azure_queue(queue_name, formatted_messages)
#     except Exception as e:
#         logging.error(f"[Fine Tuning Task] FAILURE: Queue dispatch failed: {e}", exc_info=True)
#         raise e
    
#     # ==========================================
#     # STEP 8: Save checkpoint
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 8: Saving checkpoint...")
#     if is_capped:
#         logging.info(f"[Fine Tuning Task] Capped run - checkpoint: '{last_dispatched_id}'")
    
#     try:
#         _save_checkpoint_to_langfuse(checkpoint_dataset_name, langfuse_environment, last_dispatched_id)
#     except Exception as e:
#         logging.error(f"[Fine Tuning Task] FAILURE: Checkpoint save failed: {e}", exc_info=True)
#         raise e
    
#     logging.info("=============================================================")
#     logging.info("[Fine Tuning Task] SUCCESS: IDP Fine Tuning workflow completed.")
#     logging.info("=============================================================")


# if __name__ == "__main__":
#     logging.info("Running IDP Fine Tuning data push manually...")
#     try:
#         idp_fine_tuning_data_push()
#     except Exception as main_err:
#         logging.critical(f"FATAL: {main_err}", exc_info=True)
#         raise main_err



#!/usr/bin/env python3
"""
Generate IDP fine-tuning data with Langfuse Checkpointing.

For each record in the MS SQL view `dbo.vw_PdfClassificationTransactionLog`:
    1. Load the last processed classification transaction ID from ClickHouse.
  2. Query DB for records with ID > last_checkpoint_id.
  3. Parse the ResponsePayload (AI's extraction metadata and ground truth).
  4. Extract file name, client code, and prediction from the payload.
  5. Dispatch formatted payloads to the Service Bus queue.
    6. Save the new highest processed ID back to ClickHouse.
"""

import os
import pyodbc
import logging
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List

try:
    from azure.servicebus import ServiceBusClient, ServiceBusMessage
except ImportError:
    ServiceBusClient = None
    ServiceBusMessage = None

from clickhouse_store import IDP_FINETUNING_CHECKPOINT_TABLE, get_environment, load_checkpoint_int, save_checkpoint_int

# Load dotenv if available to match accuracy and downstream environment loading
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==========================================
# SETUP DUAL-LOGGING (CONSOLE & FILE)
# ==========================================

DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "idp_fine_tuning_data_push.log")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", DEFAULT_LOG_FILE)
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

file_write_success = False
try:
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    file_write_success = True
except Exception as log_ex:
    logging.error(f"Failed to initialize log file at '{LOG_FILE_PATH}': {log_ex}")

logging.info("=============================================================")
logging.info(f"LOGGING INITIALIZED. Level: {logging.getLevelName(LOG_LEVEL)}")
if file_write_success:
    logging.info(f"Log file: {LOG_FILE_PATH}")
logging.info("=============================================================")

def _mask_value(val: Optional[str]) -> str:
    """Masks secret values in logs."""
    if not val:
        return "Not Set"
    val = val.strip()
    if len(val) <= 4:
        return "****"
    return f"{val[:2]}...{val[-2:]}"


def _extract_filename(path_value: str) -> str:
    """Extracts raw filename from a remote URL path."""
    val = str(path_value).strip() if path_value is not None else ""
    if val:
        name = Path(val).name
        if name:
            return name
    return "unknown_file.pdf"


# ==========================================
# CLICKHOUSE CHECKPOINT UTILITIES
# ==========================================


def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[int]:
    """Retrieve last processed ID from ClickHouse."""
    logging.info(f"[ClickHouse] Loading checkpoint from table '{checkpoint_dataset_name}'...")
    try:
        last_id = load_checkpoint_int(IDP_FINETUNING_CHECKPOINT_TABLE, get_environment())
        if last_id is None:
            logging.info("[ClickHouse] Checkpoint table empty. Clean start.")
            return None
        logging.info(f"[ClickHouse] SUCCESS: Retrieved checkpoint last_id='{last_id}'")
        return last_id
    except Exception as ex:
        logging.warning(f"[ClickHouse] Could not retrieve checkpoint: {ex}")
    
    return None


def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, last_id: int) -> None:
    """Save last processed ID to ClickHouse."""
    logging.info(f"[ClickHouse] Saving checkpoint: last_id='{last_id}'...")
    try:
        save_checkpoint_int(IDP_FINETUNING_CHECKPOINT_TABLE, get_environment(), last_id)
        logging.info(f"[ClickHouse] SUCCESS: Checkpoint saved with last_id='{last_id}'")
    except Exception as ex:
        logging.error(f"[ClickHouse] FAILURE: {ex}", exc_info=True)
        raise ex

# ==========================================
# DATABASE UTILITIES (ALIGNED TO SQL SERVER)
# ==========================================

def _get_db_connection(server: str, database: str, user: str, password: str):
    """Create MS SQL Server connection via pyodbc, identical to the accuracy script."""
    logging.info(f"[Database] Connecting to {server}/{database} via pyodbc...")
    
    if not all([server, database, user, password]):
        raise ValueError("Missing database credentials: IDP_SQL_SERVER, IDP_SQL_DATABASE, IDP_SQL_USER, IDP_SQL_PASSWORD")

    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
    installed_drivers = set(pyodbc.drivers())
    selected_driver = next((d for d in preferred_drivers if d in installed_drivers), None)
    if not selected_driver:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Install ODBC Driver 18 or 17 for SQL Server."
        )

    conn_str = (
        f"DRIVER={{{selected_driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Connect Timeout=30;"
    )
    if "ODBC Driver" in selected_driver:
        conn_str += "Encrypt=yes;TrustServerCertificate=no;"

    try:
        connection = pyodbc.connect(conn_str)
        logging.info("[Database] SUCCESS: Connected.")
        return connection
    except Exception as e:
        logging.error(f"[Database] FAILURE: {e}", exc_info=True)
        raise e


# ==========================================
# QUEUE UTILITIES
# ==========================================

def _send_to_azure_queue(queue_name: str, messages: List[str]) -> None:
    """Dispatch messages to Service Bus queue."""
    logging.info(f"[Queue] Dispatching {len(messages)} messages to '{queue_name}'...")
    connection_string = os.getenv("SERVICE_BUS_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("SERVICE_BUS_CONNECTION_STRING is missing.")
    if ServiceBusClient is None or ServiceBusMessage is None:
        raise ImportError("azure-servicebus package is not installed. Install with: pip install azure-servicebus")

    try:
        with ServiceBusClient.from_connection_string(connection_string) as client:
            with client.get_queue_sender(queue_name=queue_name) as sender:
                for idx, msg in enumerate(messages, 1):
                    sender.send_messages(ServiceBusMessage(msg))
                    logging.info(f"[Queue] Message {idx}/{len(messages)} sent.")
    except Exception as e:
        logging.error(f"[Queue] FAILURE: {e}", exc_info=True)
        raise e
    logging.info(f"[Queue] SUCCESS: All {len(messages)} messages sent.")


# ==========================================
# MAIN FINE TUNING TASK FUNCTION
# ==========================================

def idp_fine_tuning_data_push(
    ids_per_message: Optional[int] = None, 
    max_messages_per_run: Optional[int] = None
) -> None:
    """Fetch metadata from IDP database view and dispatch FineTuning message to Service Bus."""
    
    logging.info("=============================================================")
    logging.info("[Fine Tuning Task] Starting IDP Fine Tuning Data Push...")
    logging.info("=============================================================")
    
    # ==========================================
    # RUNTIME ENVIRONMENT VALIDATION
    # ==========================================
    logging.info("[Config] Validating environment...")
    
    langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "exp").strip()
    checkpoint_dataset_name = f"idp_finetuning_checkpoint_{langfuse_environment}"
    
    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
    if not queue_name:
        raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")
    
    ids_per_message_str = os.getenv("IDS_PER_MESSAGE", "10").strip()
    eff_ids_per_message = ids_per_message if ids_per_message is not None else int(ids_per_message_str or "10")
    
    max_messages_per_run_str = os.getenv("MAX_MESSAGES_PER_RUN", "").strip()
    eff_max_messages_per_run = (
        max_messages_per_run 
        if max_messages_per_run is not None 
        else (int(max_messages_per_run_str) if max_messages_per_run_str else None)
    )
    
    logging.info(f"[Config] Environment valid. IDs/msg={eff_ids_per_message}, MaxMsgs={eff_max_messages_per_run}")
    
    # ==========================================
    # STEP 1: Load checkpoint
    # ==========================================
    logging.info("[Fine Tuning Task] Step 1: Loading checkpoint...")
    last_checkpoint_id = _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
    # ==========================================
    # STEP 2: Build query (Using SQL Server syntax and view)
    # ==========================================
    logging.info("[Fine Tuning Task] Step 2: Building query...")
    
    query_params = []
    where_clause = "ResponsePayload IS NOT NULL AND ResponsePayload != ''"
    
    if last_checkpoint_id is not None:
        logging.info(f"[Fine Tuning Task] Incremental: last_checkpoint_id='{last_checkpoint_id}'")
        where_clause += " AND Id > ?"
        query_params.append(last_checkpoint_id)
    else:
        logging.info("[Fine Tuning Task] Full dataset query (no checkpoint)")
    
    query = (
        "SELECT "
        "  Id, "
        "  ResponsePayload "
        "FROM dbo.vw_PdfClassificationTransactionLog " 
        f"WHERE {where_clause} "
        "ORDER BY Id ASC;"
    )
    
    # ==========================================
    # STEP 3: Fetch from database
    # ==========================================
    logging.info("[Fine Tuning Task] Step 3: Executing database query...")
    server = os.getenv("IDP_SQL_SERVER")
    database = os.getenv("IDP_SQL_DATABASE")
    user = os.getenv("IDP_SQL_USER")
    password = os.getenv("IDP_SQL_PASSWORD", "")
    
    try:
        with _get_db_connection(server, database, user, password) as conn:
            cursor = conn.cursor()
            cursor.execute(query, query_params)
            rows = cursor.fetchall()
    except Exception as e:
        logging.error(f"[Fine Tuning Task] FAILURE: Database query failed: {e}", exc_info=True)
        raise e
    
    # ==========================================
    # STEP 4: Check if empty
    # ==========================================
    if len(rows) == 0:
        logging.info("[Fine Tuning Task] No new IDP records. Done.")
        return
    
    # ==========================================
    # STEP 4.5: Deduplicate and parse records
    # ==========================================
    processed_records = []
    seen_ids = set()
    skipped = 0

    for row in rows:
        record_id = row[0]
        if record_id is None or record_id in seen_ids:
            continue
        seen_ids.add(record_id)
        
        raw_json_str = row[1]  # ResponsePayload

        try:
            parsed_ground_truth = json.loads(raw_json_str)
        except json.JSONDecodeError as ex:
            logging.info(f"  SKIP TransactionId={record_id}: ResponsePayload parse error – {ex}")
            skipped += 1
            continue
            
        file_name = "unknown_file.pdf"
        prediction = "Unknown"
        client_code = "Unknown"

        if isinstance(parsed_ground_truth, dict):
            # 1. Fallback initialization from root keys if they exist
            client_code = parsed_ground_truth.get("client_code") or parsed_ground_truth.get("clientCode") or client_code

            # 2. Extract nested info from the first index of the "json" array
            if isinstance(parsed_ground_truth.get("json"), list) and parsed_ground_truth["json"]:
                first_item = parsed_ground_truth["json"][0]
                if isinstance(first_item, dict):
                    file_name = (
                        first_item.get("File Name")
                        or first_item.get("File_name")
                        or first_item.get("file_name")
                        or first_item.get("fileName")
                        or file_name
                    )
                    client_code = (
                        first_item.get("Client Code")
                        or first_item.get("Client Name")
                        or first_item.get("Client")
                        or first_item.get("ClientName")
                        or first_item.get("client_name")
                        or first_item.get("client_code")
                        or first_item.get("clientCode")
                        or client_code
                    )
                    prediction = (
                        first_item.get("Predicted Category")
                        or first_item.get("predicted_category")
                        or first_item.get("prediction")
                        or prediction
                    )

            # 3. Final root file name fallbacks if not found in the nested json
            if file_name == "unknown_file.pdf":
                file_name = (
                    parsed_ground_truth.get("file_name")
                    or parsed_ground_truth.get("fileName")
                    or parsed_ground_truth.get("File Name")
                    or parsed_ground_truth.get("File_name")
                    or parsed_ground_truth.get("File_url")
                    or "unknown_file.pdf"
                )
            
            # Use utility to clean filename structure
            file_name = _extract_filename(file_name)

        processed_records.append({
            "allocation_id": record_id,
            "file_name": file_name,
            "client_code": client_code,
            "prediction": prediction,
            "ground_truth": parsed_ground_truth
        })

    logging.info(f"[Fine Tuning Task] Parsed {len(processed_records)} valid records. Skipped: {skipped}")
    
    # ==========================================
    # STEP 5: Batch IDs
    # ==========================================
    logging.info("[Fine Tuning Task] Step 5: Batching records...")
    chunks = [processed_records[i:i + eff_ids_per_message] for i in range(0, len(processed_records), eff_ids_per_message)]
    
    is_capped = False
    if eff_max_messages_per_run is not None and len(chunks) > eff_max_messages_per_run:
        logging.warning(f"[Fine Tuning Task] CAPPING: {len(chunks)} chunks > {eff_max_messages_per_run} max")
        chunks = chunks[:eff_max_messages_per_run]
        is_capped = True
    
    logging.info(f"[Fine Tuning Task] Batched into {len(chunks)} message(s).")
    
    # ==========================================
    # STEP 6: Format messages with Service Bus schema
    # ==========================================
    logging.info("[Fine Tuning Task] Step 6: Formatting messages...")
    formatted_messages = []
    for chunk in chunks:
        # Extract corresponding lists representing the current chunk of records
        allocation_ids = [r["allocation_id"] for r in chunk]
        file_names = [r["file_name"] for r in chunk]
        client_codes = [r["client_code"] for r in chunk]
        predictions = [r["prediction"] for r in chunk]
        
        payload = {
            "file_names": file_names,
            "allocation_ids": allocation_ids,
            "client_code": client_codes,
            "ground_truth": predictions,
            "records": chunk,
            "source": "idp",
            "environment": langfuse_environment,
            "process_type": "FineTuning",
            "queued_at": datetime.now(timezone.utc).isoformat()
        }
        formatted_messages.append(json.dumps(payload))
    
    last_dispatched_id = chunks[-1][-1]["allocation_id"]
    logging.info(f"[Fine Tuning Task] Last ID: '{last_dispatched_id}'")
    
    # ==========================================
    # STEP 7: Dispatch to queue
    # ==========================================
    logging.info(f"[Fine Tuning Task] Step 7: Dispatching {len(formatted_messages)} message(s)...")
    try:
        _send_to_azure_queue(queue_name, formatted_messages)
    except Exception as e:
        logging.error(f"[Fine Tuning Task] FAILURE: Queue dispatch failed: {e}", exc_info=True)
        raise e
    
    # ==========================================
    # STEP 8: Save checkpoint
    # ==========================================
    logging.info("[Fine Tuning Task] Step 8: Saving checkpoint...")
    if is_capped:
        logging.info(f"[Fine Tuning Task] Capped run - checkpoint: '{last_dispatched_id}'")
    
    try:
        _save_checkpoint_to_langfuse(checkpoint_dataset_name, langfuse_environment, last_dispatched_id)
    except Exception as e:
        logging.error(f"[Fine Tuning Task] FAILURE: Checkpoint save failed: {e}", exc_info=True)
        raise e
    
    logging.info("=============================================================")
    logging.info("[Fine Tuning Task] SUCCESS: IDP Fine Tuning workflow completed.")
    logging.info("=============================================================")


if __name__ == "__main__":
    logging.info("Running IDP Fine Tuning data push manually...")
    try:
        idp_fine_tuning_data_push()
    except Exception as main_err:
        logging.critical(f"FATAL: {main_err}", exc_info=True)
        raise main_err