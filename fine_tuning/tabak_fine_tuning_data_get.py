

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

# DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "tabak_fine_tuning_data_push.log")
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

# from clickhouse_store import TABAK_FINETUNING_CHECKPOINT_TABLE, get_environment, load_checkpoint_str, save_checkpoint_str


# def _mask_value(val: Optional[str]) -> str:
#     """Masks secret values in logs."""
#     if not val:
#         return "Not Set"
#     val = val.strip()
#     if len(val) <= 4:
#         return "****"
#     return f"{val[:2]}...{val[-2:]}"


# def _get_oldest_date_from_db(server: str, port: str, database: str, user: str, password: str) -> Optional[str]:
#     """Retrieve the oldest date from Tabak database for records with template_info."""
#     logging.info("[Database] Fetching oldest date from database...")
#     try:
#         connection = _get_db_connection(server, port, database, user, password)
#         cursor = connection.cursor()
        
#         # Query for oldest date_created where template_info is valid
#         query = (
#             "SELECT MIN(DATE(date_created)) as oldest_date FROM `Transactions` "
#             "WHERE template_info IS NOT NULL AND template_info != '' AND JSON_VALID(template_info) "
#             "LIMIT 1"
#         )
#         cursor.execute(query)
#         result = cursor.fetchone()
#         cursor.close()
#         connection.close()
        
#         if result and result[0]:
#             oldest_date_str = f"{result[0]} 00:00:00"
#             logging.info(f"[Database] SUCCESS: Oldest date found: {oldest_date_str}")
#             return oldest_date_str
#         else:
#             logging.warning("[Database] No records found with valid template_info")
#             return None
#     except Exception as e:
#         logging.error(f"[Database] Failed to fetch oldest date: {e}", exc_info=True)
#         return None



# def _canonical_category(value) -> str:
#     """Standardizes Category strings [3]."""
#     raw = str(value).strip() if value is not None else ""
#     key = raw.lower().replace("_", "").replace(" ", "")
#     mapping = {
#         "varatingdecision": "VA_Rating_Decision",
#         "vafeeletter": "VA_Fee_Letter",
#         "other": "Others",
#         "others": "Others",
#     }
#     return mapping.get(key, raw)


# def _extract_filename(path_value: str) -> str:
#     """Extracts raw filename from a remote URL path [3]."""
#     val = str(path_value).strip() if path_value is not None else ""
#     prefix = "https://tabakprod.blob.core.windows.net/processed-files/"
#     if val.startswith(prefix):
#         val = val.replace(prefix, "", 1)
#     if val:
#         name = Path(val).name
#         if name:
#             return name
#     return "unknown_file.pdf"


# # ==========================================
# # CLICKHOUSE CHECKPOINT UTILITIES
# # ==========================================


# def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[str]:
#     """Retrieve last processed ID from ClickHouse using folder-specific table."""
#     logging.info(f"[ClickHouse] Loading checkpoint from table '{checkpoint_dataset_name}'...")
#     try:
#         last_id = load_checkpoint_str(checkpoint_dataset_name, get_environment())
#         if last_id is None:
#             logging.info("[ClickHouse] Checkpoint table empty. Clean start.")
#             return None
#         logging.info(f"[ClickHouse] SUCCESS: Retrieved checkpoint last_id='{last_id}'")
#         return last_id
#     except Exception as ex:
#         logging.warning(f"[ClickHouse] Could not retrieve checkpoint: {ex}")
    
#     return None


# def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, last_id: str) -> None:
#     """Save last processed ID to ClickHouse using folder-specific table."""
#     logging.info(f"[ClickHouse] Saving checkpoint: last_id='{last_id}'...")
#     try:
#         save_checkpoint_str(checkpoint_dataset_name, get_environment(), last_id)
#         logging.info(f"[ClickHouse] SUCCESS: Checkpoint saved with last_id='{last_id}'")
#     except Exception as ex:
#         logging.error(f"[ClickHouse] FAILURE: {ex}", exc_info=True)
#         raise ex

# # ==========================================
# # DATABASE UTILITIES
# # ==========================================

# def _get_db_connection(server: str, port: str, database: str, user: str, password: str):
#     """Create MySQL/MariaDB connection."""
#     logging.info(f"[Database] Connecting to {server}:{port}/{database}...")
    
#     if not all([server, database, user, password]):
#         raise ValueError("Missing database credentials: TABAK_DB_SERVER, TABAK_DB_DATABASE, TABAK_DB_USERID, TABAK_DB_PASSWORD")

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
#                     sender.send_messages([ServiceBusMessage(msg)])
#                     logging.info(f"[Queue] Message {idx}/{len(messages)} sent.")
#     except Exception as e:
#         logging.error(f"[Queue] FAILURE: {e}", exc_info=True)
#         raise e
#     logging.info(f"[Queue] SUCCESS: All {len(messages)} messages sent.")


# # ==========================================
# # MAIN FINE TUNING TASK FUNCTION
# # ==========================================

# def tabak_fine_tuning_data_push(
#     ids_per_message: Optional[int] = None, 
#     max_messages_per_run: Optional[int] = None,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     folder_name: Optional[str] = None,
#     bypass_checkpoint: bool = False
# ) -> None:
#     """Fetch metadata from Tabak database and dispatch FineTuning message to Service Bus.
    
#     Args:
#         ids_per_message: IDs per message (optional, uses env if not provided)
#         max_messages_per_run: Max messages per run (optional, uses env if not provided)
#         start_date: Start date in format 'YYYY-MM-DD HH:MM:SS' (optional)
#         end_date: End date in format 'YYYY-MM-DD HH:MM:SS' (optional)
#         folder_name: Logical folder/group name for checkpoint isolation and output (default: 'main')
#         bypass_checkpoint: If True, ignore checkpoint and use start_date/end_date; if False, compare checkpoint with start_date
#     """
    
#     logging.info("=============================================================")
#     logging.info("[Fine Tuning Task] Starting Tabak Fine Tuning Data Push...")
#     logging.info("=============================================================")
    
#     # ==========================================
#     # RUNTIME ENVIRONMENT VALIDATION
#     # ==========================================
#     logging.info("[Config] Validating environment...")
    
#     clickhouse_environment = get_environment()
    
#     queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
#     if not queue_name:
#         raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")
    
#     ids_per_message_str = os.getenv("IDS_PER_MESSAGE", "").strip()
#     if ids_per_message is not None:
#         eff_ids_per_message = ids_per_message
#     elif ids_per_message_str:
#         eff_ids_per_message = int(ids_per_message_str)
#     else:
#         eff_ids_per_message = 10  # default

#     if max_messages_per_run is not None:
#         # Treat -1 as unlimited (no limit)
#         eff_max_messages_per_run = None if max_messages_per_run == -1 else max_messages_per_run
#     else:
#         max_messages_per_run_str = os.getenv("MAX_MESSAGES_PER_RUN", "").strip()
#         eff_max_messages_per_run = int(max_messages_per_run_str) if max_messages_per_run_str else None
    
#     # Container name always comes from credentials/env vars
#     _container_name = os.getenv("TABAK_CONTAINER", "tabak-dataset").strip()
#     logging.info(f"[Config] Using container_name: {_container_name}")
    
#     # folder_name isolates checkpoint and output data — defaults to 'main'
#     folder_name = (folder_name or "main").strip()
#     logging.info(f"[Config] Using folder_name: {folder_name}")
    
#     # Create folder-specific checkpoint table name (auto-created in ClickHouse on first use)
#     checkpoint_dataset_name = f"tabak_finetuning_checkpoint_{folder_name}"
#     logging.info(f"[Config] Using checkpoint table: {checkpoint_dataset_name}")
    
#     # Get database connection details
#     db_server = os.getenv("TABAK_DB_SERVER", "").strip()
#     db_port = os.getenv("TABAK_DB_PORT", "3306").strip()
#     db_database = os.getenv("TABAK_DB_DATABASE", "").strip()
#     db_user = os.getenv("TABAK_DB_USERID", "").strip()
#     db_password = os.getenv("TABAK_DB_PASSWORD", "").strip()
    
#     # Handle dates and checkpoint logic
#     logging.info(f"[Config] start_date={start_date}, end_date={end_date}, bypass_checkpoint={bypass_checkpoint}")
    
#     effective_start_date = start_date
#     effective_end_date = end_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
#     # If start_date not provided, fetch oldest date from database
#     if not effective_start_date:
#         logging.info("[Config] start_date not provided, fetching oldest date from database...")
#         effective_start_date = _get_oldest_date_from_db(db_server, db_port, db_database, db_user, db_password)
#         if effective_start_date:
#             logging.info(f"[Config] Using oldest date from database: {effective_start_date}")
#         else:
#             logging.warning("[Config] Could not determine oldest date, using current date")
#             effective_start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
#     logging.info(f"[Config] Environment valid. IDs/msg={eff_ids_per_message}, MaxMsgs={eff_max_messages_per_run}")
    
#     # ==========================================
#     # STEP 1: Load checkpoint and determine effective start date
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 1: Loading checkpoint...")
#     last_checkpoint_id = None if bypass_checkpoint else _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
#     if bypass_checkpoint:
#         logging.info("[Fine Tuning Task] Checkpoint bypass is enabled, using provided start_date/end_date")
    
#     # ==========================================
#     # STEP 2: Build query with JSON filters
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 2: Building query...")
    
#     filters = [
#         "Transation_id IS NOT NULL",
#         "template_info IS NOT NULL",
#         "template_info != ''",
#         "JSON_VALID(template_info)",
#         "JSON_EXISTS(template_info, '$.generated_response.VADetails.Category')",
#         "JSON_EXISTS(template_info, '$.generated_response.VADetails.Subcategory')",
#         "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Category')",
#         "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Subcategory')",
#     ]
    
#     query_params = []
#     if last_checkpoint_id:
#         logging.info(f"[Fine Tuning Task] Incremental: last_checkpoint_id='{last_checkpoint_id}'")
#         filters.append("Transation_id > %s")
#         query_params.append(last_checkpoint_id)
#         logging.info(f"[Fine Tuning Task] Applied checkpoint filter")
#     else:
#         logging.info("[Fine Tuning Task] Full dataset query (no checkpoint)")
    
#     where_clause = " AND ".join(filters)
#     logging.info(f"[Fine Tuning Task] WHERE clause: {where_clause[:100]}...")
    
#     # Query targets file path, and both category layers for overrides
#     query = (
#         "SELECT "
#         "  Transation_id, "
#         "  JSON_VALUE(template_info, '$.generated_response.VADetails.Category') AS gen_cat, "
#         "  JSON_VALUE(template_info, '$.generated_response.VADetails.Subcategory') AS gen_sub, "
#         "  JSON_VALUE(template_info, '$.user_selected_response.VADetails.Category') AS user_cat, "
#         "  JSON_VALUE(template_info, '$.user_selected_response.VADetails.Subcategory') AS user_sub, "
#         "  COALESCE("
#         "    JSON_VALUE(template_info, '$.generated_response.VADetails.file_path'),"
#         "    JSON_VALUE(template_info, '$.file_path')"
#         "  ) AS file_path "
#         "FROM `Transactions` "
#         f"WHERE {where_clause} "
#         "ORDER BY Transation_id ASC;"
#     )
    
#     # ==========================================
#     # STEP 3: Fetch from database
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 3: Executing database query...")
#     server = os.getenv("TABAK_DB_SERVER")
#     port = os.getenv("TABAK_DB_PORT", "3306")
#     database = os.getenv("TABAK_DB_DATABASE")
#     user = os.getenv("TABAK_DB_USERID")
#     password = os.getenv("TABAK_DB_PASSWORD")
    
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
#         logging.info("[Fine Tuning Task] No new records. Done.")
#         return
    
#     # ==========================================
#     # STEP 4.5: Deduplicate and parse records
#     # ==========================================
#     processed_records = []
#     seen_ids = set()
#     for row in rows:
#         t_id = str(row[0]).strip() if row[0] is not None else None
#         if not t_id or t_id in seen_ids:
#             continue
#         seen_ids.add(t_id)
        
#         gen_cat = _canonical_category(row[1])
#         gen_sub = str(row[2]).strip() if row[2] is not None else ""
#         user_cat = _canonical_category(row[3])
#         user_sub = str(row[4]).strip() if row[4] is not None else ""
#         raw_file_path = str(row[5]).strip() if row[5] is not None else ""
        
#         has_user_override = bool(user_cat or user_sub)
#         final_cat = user_cat if has_user_override else gen_cat
#         final_sub = user_sub if has_user_override else gen_sub
        
#         # Determine prediction validity [3]
#         is_correct = (gen_cat == final_cat) and (gen_sub == final_sub)
        
#         filename = _extract_filename(raw_file_path)
        
#         processed_records.append({
#             "record_id": t_id,
#             "filename": filename,
#             "category": final_cat,
#             "subcategory": final_sub,
#             "is_correct": is_correct
#         })

#     logging.info(f"[Fine Tuning Task] Parsed {len(processed_records)} unique records.")
    
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
#     # STEP 6 & 7: Format and send messages in batches
#     # ==========================================
#     logging.info("[Fine Tuning Task] Step 6-7: Formatting and dispatching messages in batches...")
    
#     total_sent = 0
#     batch_dispatch_size = 10  # Send 10 messages at a time
#     last_dispatched_id = None
    
#     for batch_start in range(0, len(chunks), batch_dispatch_size):
#         batch_end = min(batch_start + batch_dispatch_size, len(chunks))
#         batch_chunks = chunks[batch_start:batch_end]
#         batch_num = (batch_start // batch_dispatch_size) + 1
#         total_batches = (len(chunks) + batch_dispatch_size - 1) // batch_dispatch_size
        
#         logging.info(f"[Fine Tuning Task] Sending batch {batch_num}/{total_batches} ({len(batch_chunks)} messages)...")
        
#         formatted_messages = []
#         for chunk in batch_chunks:
#             record_ids = [r["record_id"] for r in chunk]
#             filenames = [r["filename"] for r in chunk]
#             ground_truth = [
#                 {
#                     "category": r["category"], 
#                     "subcategory": r["subcategory"],
#                     "is_correct": r["is_correct"]
#                 } 
#                 for r in chunk
#             ]
            
#             payload = {
#                 "record_ids": record_ids,
#                 "File name": filenames,
#                 "Ground_truth": ground_truth,
#                 "source": "tabak",
#                 "container": _container_name,
#                 "folder_name": folder_name,
#                 "environment": clickhouse_environment,
#                 "process_type": "FineTuning",
#                 "queued_at": datetime.now(timezone.utc).isoformat()
#             }
#             formatted_messages.append(json.dumps(payload))
        
#         last_dispatched_id = batch_chunks[-1][-1]["record_id"]
        
#         try:
#             _send_to_azure_queue(queue_name, formatted_messages)
#             total_sent += len(formatted_messages)
#             logging.info(f"[Fine Tuning Task] Batch {batch_num} sent. Last ID: '{last_dispatched_id}'")
            
#             # Save checkpoint after each batch
#             try:
#                 _save_checkpoint_to_langfuse(checkpoint_dataset_name, clickhouse_environment, last_dispatched_id)
#                 logging.info(f"[Fine Tuning Task] Checkpoint saved after batch {batch_num}")
#             except Exception as cp_ex:
#                 logging.warning(f"[Fine Tuning Task] Failed to save checkpoint: {cp_ex}")
                
#         except Exception as e:
#             logging.error(f"[Fine Tuning Task] FAILURE sending batch {batch_num}: {e}", exc_info=True)
#             # Continue with next batch even if this one fails
#             continue
    
#     is_capped = False
#     if eff_max_messages_per_run is not None and len(chunks) > eff_max_messages_per_run:
#         is_capped = True
    
#     logging.info(f"[Fine Tuning Task] Completed. Sent {total_sent} message(s)")
    
#     logging.info("=============================================================")
#     logging.info("[Fine Tuning Task] SUCCESS: Tabak Fine Tuning workflow completed.")
#     logging.info("=============================================================")


# if __name__ == "__main__":
#     logging.info("Running Tabak Fine Tuning data push manually...")
#     try:
#         tabak_fine_tuning_data_push()
#     except Exception as main_err:
#         logging.critical(f"FATAL: {main_err}", exc_info=True)
#         raise main_err


import os
import pymysql
import logging
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ==========================================
# SETUP DUAL-LOGGING (CONSOLE & FILE)
# ==========================================

DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "tabak_fine_tuning_data_push.log")
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

from clickhouse_store import TABAK_FINETUNING_CHECKPOINT_TABLE, get_environment, load_checkpoint_str, save_checkpoint_str


def _mask_value(val: Optional[str]) -> str:
    """Masks secret values in logs."""
    if not val:
        return "Not Set"
    val = val.strip()
    if len(val) <= 4:
        return "****"
    return f"{val[:2]}...{val[-2:]}"


def _get_oldest_date_from_db(server: str, port: str, database: str, user: str, password: str) -> Optional[str]:
    """Retrieve the oldest date from Tabak database for records with template_info."""
    logging.info("[Database] Fetching oldest date from database...")
    try:
        connection = _get_db_connection(server, port, database, user, password)
        cursor = connection.cursor()
        
        # Query for oldest date_created where template_info is valid
        query = (
            "SELECT MIN(DATE(date_created)) as oldest_date FROM `Transactions` "
            "WHERE template_info IS NOT NULL AND template_info != '' AND JSON_VALID(template_info) "
            "LIMIT 1"
        )
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if result and result[0]:
            oldest_date_str = f"{result[0]} 00:00:00"
            logging.info(f"[Database] SUCCESS: Oldest date found: {oldest_date_str}")
            return oldest_date_str
        else:
            logging.warning("[Database] No records found with valid template_info")
            return None
    except Exception as e:
        logging.error(f"[Database] Failed to fetch oldest date: {e}", exc_info=True)
        return None



def _canonical_category(value) -> str:
    """Standardizes Category strings [3]."""
    raw = str(value).strip() if value is not None else ""
    key = raw.lower().replace("_", "").replace(" ", "")
    mapping = {
        "varatingdecision": "VA_Rating_Decision",
        "vafeeletter": "VA_Fee_Letter",
        "other": "Others",
        "others": "Others",
    }
    return mapping.get(key, raw)


def _extract_filename(path_value: str) -> str:
    """Extracts raw filename from a remote URL path [3]."""
    val = str(path_value).strip() if path_value is not None else ""
    prefix = "https://tabakprod.blob.core.windows.net/processed-files/"
    if val.startswith(prefix):
        val = val.replace(prefix, "", 1)
    if val:
        name = Path(val).name
        if name:
            return name
    return "unknown_file.pdf"


# ==========================================
# CLICKHOUSE CHECKPOINT UTILITIES
# ==========================================


def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[str]:
    """Retrieve last processed ID from ClickHouse using folder-specific table."""
    logging.info(f"[ClickHouse] Loading checkpoint from table '{checkpoint_dataset_name}'...")
    try:
        last_id = load_checkpoint_str(checkpoint_dataset_name, get_environment())
        if last_id is None:
            logging.info("[ClickHouse] Checkpoint table empty. Clean start.")
            return None
        logging.info(f"[ClickHouse] SUCCESS: Retrieved checkpoint last_id='{last_id}'")
        return last_id
    except Exception as ex:
        logging.warning(f"[ClickHouse] Could not retrieve checkpoint: {ex}")
    
    return None


def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, last_id: str) -> None:
    """Save last processed ID to ClickHouse using folder-specific table."""
    logging.info(f"[ClickHouse] Saving checkpoint: last_id='{last_id}'...")
    try:
        save_checkpoint_str(checkpoint_dataset_name, get_environment(), last_id)
        logging.info(f"[ClickHouse] SUCCESS: Checkpoint saved with last_id='{last_id}'")
    except Exception as ex:
        logging.error(f"[ClickHouse] FAILURE: {ex}", exc_info=True)
        raise ex

# ==========================================
# DATABASE UTILITIES
# ==========================================

def _get_db_connection(server: str, port: str, database: str, user: str, password: str):
    """Create MySQL/MariaDB connection."""
    logging.info(f"[Database] Connecting to {server}:{port}/{database}...")
    
    if not all([server, database, user, password]):
        raise ValueError("Missing database credentials: TABAK_DB_SERVER, TABAK_DB_DATABASE, TABAK_DB_USERID, TABAK_DB_PASSWORD")

    try:
        connection = pymysql.connect(
            host=server,
            port=int(port),
            user=user,
            password=password,
            database=database,
            connect_timeout=30,
            charset="utf8mb4"
        )
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

    try:
        with ServiceBusClient.from_connection_string(connection_string) as client:
            with client.get_queue_sender(queue_name=queue_name) as sender:
                for idx, msg in enumerate(messages, 1):
                    sender.send_messages([ServiceBusMessage(msg)])
                    logging.info(f"[Queue] Message {idx}/{len(messages)} sent.")
    except Exception as e:
        logging.error(f"[Queue] FAILURE: {e}", exc_info=True)
        raise e
    logging.info(f"[Queue] SUCCESS: All {len(messages)} messages sent.")


# ==========================================
# MAIN FINE TUNING TASK FUNCTION
# ==========================================

def tabak_fine_tuning_data_push(
    ids_per_message: Optional[int] = None, 
    max_messages_per_run: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    folder_name: Optional[str] = None,
    bypass_checkpoint: bool = False
) -> None:
    """Fetch metadata from Tabak database and dispatch FineTuning message to Service Bus.
    
    Args:
        ids_per_message: IDs per message (optional, uses env if not provided)
        max_messages_per_run: Max messages per run (optional, uses env if not provided)
        start_date: Start date in format 'YYYY-MM-DD HH:MM:SS' (optional)
        end_date: End date in format 'YYYY-MM-DD HH:MM:SS' (optional)
        folder_name: Logical folder/group name for checkpoint isolation and output (default: 'main')
        bypass_checkpoint: If True, ignore checkpoint and use start_date/end_date; if False, compare checkpoint with start_date
    """
    
    logging.info("=============================================================")
    logging.info("[Fine Tuning Task] Starting Tabak Fine Tuning Data Push...")
    logging.info("=============================================================")
    
    # ==========================================
    # RUNTIME ENVIRONMENT VALIDATION
    # ==========================================
    logging.info("[Config] Validating environment...")
    
    clickhouse_environment = get_environment()
    
    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
    if not queue_name:
        raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")
    
    ids_per_message_str = os.getenv("IDS_PER_MESSAGE", "").strip()
    if ids_per_message is not None:
        eff_ids_per_message = ids_per_message
    elif ids_per_message_str:
        eff_ids_per_message = int(ids_per_message_str)
    else:
        eff_ids_per_message = 10  # default

    if max_messages_per_run is not None:
        # Treat -1 as unlimited (no limit)
        eff_max_messages_per_run = None if max_messages_per_run == -1 else max_messages_per_run
    else:
        max_messages_per_run_str = os.getenv("MAX_MESSAGES_PER_RUN", "").strip()
        eff_max_messages_per_run = int(max_messages_per_run_str) if max_messages_per_run_str else None
    
    # Container name always comes from credentials/env vars
    _container_name = os.getenv("TABAK_CONTAINER", "tabak-dataset").strip()
    logging.info(f"[Config] Using container_name: {_container_name}")
    
    # folder_name isolates checkpoint and output data — defaults to 'main'
    folder_name = (folder_name or "main").strip()
    logging.info(f"[Config] Using folder_name: {folder_name}")
    
    # Create folder-specific checkpoint table name (auto-created in ClickHouse on first use)
    checkpoint_dataset_name = f"tabak_finetuning_checkpoint_{folder_name}"
    logging.info(f"[Config] Using checkpoint table: {checkpoint_dataset_name}")
    
    # Get database connection details
    db_server = os.getenv("TABAK_DB_SERVER", "").strip()
    db_port = os.getenv("TABAK_DB_PORT", "3306").strip()
    db_database = os.getenv("TABAK_DB_DATABASE", "").strip()
    db_user = os.getenv("TABAK_DB_USERID", "").strip()
    db_password = os.getenv("TABAK_DB_PASSWORD", "").strip()
    
    # Handle dates and checkpoint logic
    logging.info(f"[Config] start_date={start_date}, end_date={end_date}, bypass_checkpoint={bypass_checkpoint}")
    
    effective_end_date = end_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"[Config] Environment valid. End Date={effective_end_date}, IDs/msg={eff_ids_per_message}, MaxMsgs={eff_max_messages_per_run}")
    
    # ==========================================
    # STEP 1: Load checkpoint
    # ==========================================
    logging.info("[Fine Tuning Task] Step 1: Loading checkpoint...")
    last_checkpoint_id = None if bypass_checkpoint else _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
    if bypass_checkpoint:
        logging.info("[Fine Tuning Task] Checkpoint bypass is enabled.")
    
    # ==========================================
    # STEP 2: Build query with JSON and ID filters
    # ==========================================
    logging.info("[Fine Tuning Task] Step 2: Building query...")
    
    BASELINE_TRANSACTION_ID = 33062  # Hard minimum — never process records at or below this ID
    
    # Effective lower bound: whichever is higher — baseline or checkpoint
    checkpoint_int = int(last_checkpoint_id) if last_checkpoint_id else 0
    effective_min_id = max(BASELINE_TRANSACTION_ID, checkpoint_int)
    logging.info(f"[Fine Tuning Task] Effective min Transation_id: {effective_min_id} "
                 f"(baseline={BASELINE_TRANSACTION_ID}, checkpoint={checkpoint_int})")
    
    filters = [
        "Transation_id IS NOT NULL",
        "template_info IS NOT NULL",
        "template_info != ''",
        "JSON_VALID(template_info)",
        "JSON_EXISTS(template_info, '$.generated_response.VADetails.Category')",
        "JSON_EXISTS(template_info, '$.generated_response.VADetails.Subcategory')",
        "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Category')",
        "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Subcategory')",
    ]
    
    query_params = []

    # Always filter by the effective minimum ID (max of baseline and checkpoint)
    filters.append("Transation_id > %s")
    query_params.append(effective_min_id)

    # Always cap at end date
    filters.append("date_created <= %s")
    query_params.append(effective_end_date)
    logging.info(f"[Fine Tuning Task] Filter: Transation_id > {effective_min_id}, date_created <= {effective_end_date}")
    
    where_clause = " AND ".join(filters)
    logging.info(f"[Fine Tuning Task] WHERE clause: {where_clause[:120]}...")
    
    # Query targets file path, and both category layers for overrides
    query = (
        "SELECT "
        "  Transation_id, "
        "  JSON_VALUE(template_info, '$.generated_response.VADetails.Category') AS gen_cat, "
        "  JSON_VALUE(template_info, '$.generated_response.VADetails.Subcategory') AS gen_sub, "
        "  JSON_VALUE(template_info, '$.user_selected_response.VADetails.Category') AS user_cat, "
        "  JSON_VALUE(template_info, '$.user_selected_response.VADetails.Subcategory') AS user_sub, "
        "  COALESCE("
        "    JSON_VALUE(template_info, '$.generated_response.VADetails.file_path'),"
        "    JSON_VALUE(template_info, '$.file_path')"
        "  ) AS file_path "
        "FROM `Transactions` "
        f"WHERE {where_clause} "
        "ORDER BY Transation_id ASC;"
    )
    
    # ==========================================
    # STEP 3: Fetch from database
    # ==========================================
    logging.info("[Fine Tuning Task] Step 3: Executing database query...")
    server = os.getenv("TABAK_DB_SERVER")
    port = os.getenv("TABAK_DB_PORT", "3306")
    database = os.getenv("TABAK_DB_DATABASE")
    user = os.getenv("TABAK_DB_USERID")
    password = os.getenv("TABAK_DB_PASSWORD")
    
    try:
        with _get_db_connection(server, port, database, user, password) as conn:
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
        logging.info("[Fine Tuning Task] No new records. Done.")
        return
    
    # ==========================================
    # STEP 4.5: Deduplicate and parse records
    # ==========================================
    processed_records = []
    seen_ids = set()
    for row in rows:
        t_id = str(row[0]).strip() if row[0] is not None else None
        if not t_id or t_id in seen_ids:
            continue
        seen_ids.add(t_id)
        
        gen_cat = _canonical_category(row[1])
        gen_sub = str(row[2]).strip() if row[2] is not None else ""
        user_cat = _canonical_category(row[3])
        user_sub = str(row[4]).strip() if row[4] is not None else ""
        raw_file_path = str(row[5]).strip() if row[5] is not None else ""
        
        has_user_override = bool(user_cat or user_sub)
        final_cat = user_cat if has_user_override else gen_cat
        final_sub = user_sub if has_user_override else gen_sub
        
        # Determine prediction validity [3]
        is_correct = (gen_cat == final_cat) and (gen_sub == final_sub)
        
        filename = _extract_filename(raw_file_path)
        
        processed_records.append({
            "record_id": t_id,
            "filename": filename,
            "category": final_cat,
            "subcategory": final_sub,
            "is_correct": is_correct
        })

    logging.info(f"[Fine Tuning Task] Parsed {len(processed_records)} unique records.")
    
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
    # STEP 6 & 7: Format and send messages in batches
    # ==========================================
    logging.info("[Fine Tuning Task] Step 6-7: Formatting and dispatching messages in batches...")
    
    total_sent = 0
    batch_dispatch_size = 10  # Send 10 messages at a time
    last_dispatched_id = None
    
    for batch_start in range(0, len(chunks), batch_dispatch_size):
        batch_end = min(batch_start + batch_dispatch_size, len(chunks))
        batch_chunks = chunks[batch_start:batch_end]
        batch_num = (batch_start // batch_dispatch_size) + 1
        total_batches = (len(chunks) + batch_dispatch_size - 1) // batch_dispatch_size
        
        logging.info(f"[Fine Tuning Task] Sending batch {batch_num}/{total_batches} ({len(batch_chunks)} messages)...")
        
        formatted_messages = []
        for chunk in batch_chunks:
            record_ids = [r["record_id"] for r in chunk]
            filenames = [r["filename"] for r in chunk]
            ground_truth = [
                {
                    "category": r["category"], 
                    "subcategory": r["subcategory"],
                    "is_correct": r["is_correct"]
                } 
                for r in chunk
            ]
            
            payload = {
                "record_ids": record_ids,
                "File name": filenames,
                "Ground_truth": ground_truth,
                "source": "tabak",
                "container": _container_name,
                "folder_name": folder_name,
                "environment": clickhouse_environment,
                "process_type": "FineTuning",
                "queued_at": datetime.now(timezone.utc).isoformat()
            }
            formatted_messages.append(json.dumps(payload))
        
        last_dispatched_id = batch_chunks[-1][-1]["record_id"]
        
        try:
            _send_to_azure_queue(queue_name, formatted_messages)
            total_sent += len(formatted_messages)
            logging.info(f"[Fine Tuning Task] Batch {batch_num} sent. Last ID: '{last_dispatched_id}'")
            
            # Save checkpoint after each batch
            try:
                _save_checkpoint_to_langfuse(checkpoint_dataset_name, clickhouse_environment, last_dispatched_id)
                logging.info(f"[Fine Tuning Task] Checkpoint saved after batch {batch_num}")
            except Exception as cp_ex:
                logging.warning(f"[Fine Tuning Task] Failed to save checkpoint: {cp_ex}")
                
        except Exception as e:
            logging.error(f"[Fine Tuning Task] FAILURE sending batch {batch_num}: {e}", exc_info=True)
            # Continue with next batch even if this one fails
            continue
    
    is_capped = False
    if eff_max_messages_per_run is not None and len(chunks) > eff_max_messages_per_run:
        is_capped = True
    
    logging.info(f"[Fine Tuning Task] Completed. Sent {total_sent} message(s)")
    
    logging.info("=============================================================")
    logging.info("[Fine Tuning Task] SUCCESS: Tabak Fine Tuning workflow completed.")
    logging.info("=============================================================")


if __name__ == "__main__":
    logging.info("Running Tabak Fine Tuning data push manually...")
    try:
        tabak_fine_tuning_data_push()
    except Exception as main_err:
        logging.critical(f"FATAL: {main_err}", exc_info=True)
        raise main_err