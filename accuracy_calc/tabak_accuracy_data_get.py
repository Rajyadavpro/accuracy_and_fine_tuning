# import os
# import pymysql
# import logging
# import json
# import tempfile
# from datetime import datetime, timezone
# from typing import Optional, List
# from azure.servicebus import ServiceBusClient, ServiceBusMessage

# # ==========================================
# # SETUP DUAL-LOGGING (CONSOLE & FILE)
# # ==========================================

# DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "tabak_data_push.log")
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


# def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[str]:
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
#             last_id = str(checkpoint_data["last_id"])
#             logging.info(f"[Langfuse] SUCCESS: Retrieved checkpoint last_id='{last_id}'")
#             return last_id
#         else:
#             logging.error(f"[Langfuse] Checkpoint payload invalid: {checkpoint_data}")
#     except Exception as ex:
#         logging.warning(f"[Langfuse] Could not retrieve checkpoint: {ex}")
    
#     return None


# def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, last_id: str) -> None:
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
#             description=f"Tabak accuracy checkpoint ({langfuse_environment})"
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
#             metadata={"record_type": "tabak_checkpoint", "last_id": last_id}
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
#                     sender.send_messages(ServiceBusMessage(msg))
#                     logging.info(f"[Queue] Message {idx}/{len(messages)} sent.")
#     except Exception as e:
#         logging.error(f"[Queue] FAILURE: {e}", exc_info=True)
#         raise e
#     logging.info(f"[Queue] SUCCESS: All {len(messages)} messages sent.")


# # ==========================================
# # MAIN TASK FUNCTION
# # ==========================================

# def tabak_data_push(ids_per_message: Optional[int] = None, max_messages_per_run: Optional[int] = None) -> None:
#     """Fetch unique Transation_ids from Tabak database and dispatch to Service Bus."""
    
#     logging.info("=============================================================")
#     logging.info("[Timer Task 2] Starting Tabak Data Push...")
#     logging.info("=============================================================")
    
#     # ==========================================
#     # RUNTIME ENVIRONMENT VALIDATION
#     # ==========================================
#     logging.info("[Config] Validating environment...")
    
#     langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "").strip()
#     if not langfuse_environment:
#         raise ValueError("LANGFUSE_ENVIRONMENT must be set.")
    
#     checkpoint_dataset_name = f"tabak_accuracy_checkpoint_{langfuse_environment}"
#     queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
#     if not queue_name:
#         raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")
    
#     ids_per_message_str = os.getenv("IDS_PER_MESSAGE", "").strip()
#     if not ids_per_message_str:
#         raise ValueError("IDS_PER_MESSAGE must be set.")
#     eff_ids_per_message = int(ids_per_message_str)
#     if ids_per_message is not None:
#         eff_ids_per_message = ids_per_message
    
#     max_messages_per_run_str = os.getenv("MAX_MESSAGES_PER_RUN", "").strip()
#     if not max_messages_per_run_str:
#         raise ValueError("MAX_MESSAGES_PER_RUN must be set.")
#     eff_max_messages_per_run = int(max_messages_per_run_str)
#     if max_messages_per_run is not None:
#         eff_max_messages_per_run = max_messages_per_run
    
#     logging.info(f"[Config] Environment valid. IDs/msg={eff_ids_per_message}, MaxMsgs={eff_max_messages_per_run}")
    
#     # ==========================================
#     # STEP 1: Load checkpoint
#     # ==========================================
#     logging.info("[Timer Task 2] Step 1: Loading checkpoint...")
#     last_checkpoint_id = _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
#     # ==========================================
#     # STEP 2: Build query with JSON filters
#     # ==========================================
#     logging.info("[Timer Task 2] Step 2: Building query...")
    
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
#         logging.info(f"[Timer Task 2] Incremental: last_checkpoint_id='{last_checkpoint_id}'")
#         filters.append("Transation_id > %s")
#         query_params.append(last_checkpoint_id)
#         logging.info(f"[Timer Task 2] Applied checkpoint filter")
#     else:
#         logging.info("[Timer Task 2] Full dataset query (no checkpoint)")
    
#     where_clause = " AND ".join(filters)
#     logging.info(f"[Timer Task 2] WHERE clause: {where_clause[:100]}...")
    
#     query = (
#         "SELECT DISTINCT Transation_id "
#         "FROM `Transactions` "
#         f"WHERE {where_clause} "
#         "ORDER BY Transation_id ASC;"
#     )
    
#     # ==========================================
#     # STEP 3: Fetch from database
#     # ==========================================
#     logging.info("[Timer Task 2] Step 3: Executing database query...")
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
#             unique_keys = [str(row[0]).strip() for row in rows if row[0] is not None]
#             logging.info(f"[Timer Task 2] Retrieved {len(unique_keys)} unique IDs.")
#     except Exception as e:
#         logging.error(f"[Timer Task 2] FAILURE: Database query failed: {e}", exc_info=True)
#         raise e
    
#     # ==========================================
#     # STEP 4: Check if empty
#     # ==========================================
#     if len(unique_keys) == 0:
#         logging.info("[Timer Task 2] No new records. Done.")
#         return
    
#     # ==========================================
#     # STEP 5: Batch IDs
#     # ==========================================
#     logging.info("[Timer Task 2] Step 5: Batching IDs...")
#     chunks = [unique_keys[i:i + eff_ids_per_message] for i in range(0, len(unique_keys), eff_ids_per_message)]
    
#     is_capped = False
#     if len(chunks) > eff_max_messages_per_run:
#         logging.warning(f"[Timer Task 2] CAPPING: {len(chunks)} chunks > {eff_max_messages_per_run} max")
#         chunks = chunks[:eff_max_messages_per_run]
#         is_capped = True
    
#     logging.info(f"[Timer Task 2] Batched into {len(chunks)} message(s).")
    
#     # ==========================================
#     # STEP 6: Format messages
#     # ==========================================
#     logging.info("[Timer Task 2] Step 6: Formatting messages...")
#     formatted_messages = []
#     for chunk in chunks:
#         payload = {
#             "record_ids": chunk,
#             "source": "tabak",
#             "environment": langfuse_environment,
#             "queued_at": datetime.now(timezone.utc).isoformat()
#         }
#         formatted_messages.append(json.dumps(payload))
    
#     last_dispatched_id = chunks[-1][-1]
#     logging.info(f"[Timer Task 2] Last ID: '{last_dispatched_id}'")
    
#     # ==========================================
#     # STEP 7: Dispatch to queue
#     # ==========================================
#     logging.info(f"[Timer Task 2] Step 7: Dispatching {len(formatted_messages)} message(s)...")
#     try:
#         _send_to_azure_queue(queue_name, formatted_messages)
#     except Exception as e:
#         logging.error(f"[Timer Task 2] FAILURE: Queue dispatch failed: {e}", exc_info=True)
#         raise e
    
#     # ==========================================
#     # STEP 8: Save checkpoint
#     # ==========================================
#     logging.info("[Timer Task 2] Step 8: Saving checkpoint...")
#     if is_capped:
#         logging.info(f"[Timer Task 2] Capped run - checkpoint: '{last_dispatched_id}'")
    
#     try:
#         _save_checkpoint_to_langfuse(checkpoint_dataset_name, langfuse_environment, last_dispatched_id)
#     except Exception as e:
#         logging.error(f"[Timer Task 2] FAILURE: Checkpoint save failed: {e}", exc_info=True)
#         raise e
    
#     logging.info("=============================================================")
#     logging.info("[Timer Task 2] SUCCESS: Tabak workflow completed.")
#     logging.info("=============================================================")


# if __name__ == "__main__":
#     logging.info("Running Tabak data push manually...")
#     try:
#         tabak_data_push()
#     except Exception as main_err:
#         logging.critical(f"FATAL: {main_err}", exc_info=True)
#         raise main_err



import os
import pymysql
import logging
import json
import tempfile
from datetime import datetime, timezone
from typing import Optional, List
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ==========================================
# SETUP DUAL-LOGGING (CONSOLE & FILE)
# ==========================================

DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "tabak_data_push.log")
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

try:
    from langfuse import Langfuse
    logging.info("[Langfuse] Library imported successfully.")
except ImportError:
    logging.error("[Langfuse] FAILURE: Langfuse library not installed.")
    Langfuse = None


def _mask_value(val: Optional[str]) -> str:
    """Masks secret values in logs."""
    if not val:
        return "Not Set"
    val = val.strip()
    if len(val) <= 4:
        return "****"
    return f"{val[:2]}...{val[-2:]}"


# ==========================================
# LANGFUSE CLIENT & CHECKPOINT UTILITIES
# ==========================================

def _get_langfuse_client():
    """Initialize Langfuse client."""
    logging.info("[Langfuse] Initializing client...")
    
    if not Langfuse:
        raise ImportError("Langfuse library is not installed.")

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    host = os.getenv("LANGFUSE_HOST")

    logging.info(f"[Langfuse] Config: PK={_mask_value(public_key)}, SK={_mask_value(secret_key)}, Host={host}")

    if not all([public_key, secret_key, host]):
        raise ValueError("Missing Langfuse credentials: LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY, LANGFUSE_HOST")

    try:
        client = Langfuse(public_key=public_key.strip(), secret_key=secret_key.strip(), host=host.strip())
        logging.info("[Langfuse] SUCCESS: Client initialized.")
        return client
    except Exception as e:
        logging.error(f"[Langfuse] FAILURE: {e}", exc_info=True)
        raise e


def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[str]:
    """Retrieve last processed ID from Langfuse."""
    logging.info(f"[Langfuse] Loading checkpoint from dataset '{checkpoint_dataset_name}'...")
    try:
        langfuse = _get_langfuse_client()
    except Exception as init_err:
        logging.warning(f"[Langfuse] Could not initialize client: {init_err}. Starting clean.")
        return None

    try:
        dataset = langfuse.get_dataset(checkpoint_dataset_name)
        if not dataset or not hasattr(dataset, 'items') or not dataset.items:
            logging.info(f"[Langfuse] Dataset empty or not found. Clean start.")
            return None
        
        latest_item = max(dataset.items, key=lambda r: getattr(r, 'created_at'))
        checkpoint_data = latest_item.input
        
        if isinstance(checkpoint_data, dict) and "last_id" in checkpoint_data:
            last_id = str(checkpoint_data["last_id"])
            logging.info(f"[Langfuse] SUCCESS: Retrieved checkpoint last_id='{last_id}'")
            return last_id
        else:
            logging.error(f"[Langfuse] Checkpoint payload invalid: {checkpoint_data}")
    except Exception as ex:
        logging.warning(f"[Langfuse] Could not retrieve checkpoint: {ex}")
    
    return None


def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, last_id: str) -> None:
    """Save last processed ID to Langfuse."""
    logging.info(f"[Langfuse] Saving checkpoint: last_id='{last_id}'...")
    try:
        langfuse = _get_langfuse_client()
    except Exception as init_err:
        logging.error(f"[Langfuse] Cannot save checkpoint: {init_err}")
        raise init_err

    try:
        langfuse.create_dataset(
            name=checkpoint_dataset_name,
            description=f"Tabak accuracy checkpoint ({langfuse_environment})"
        )
        
        checkpoint_item_id = f"checkpoint::id::{last_id}"
        checkpoint_payload = {
            "last_id": last_id,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        
        langfuse.create_dataset_item(
            dataset_name=checkpoint_dataset_name,
            id=checkpoint_item_id,
            input=checkpoint_payload,
            metadata={"record_type": "tabak_checkpoint", "last_id": last_id}
        )
        
        langfuse.flush()
        logging.info(f"[Langfuse] SUCCESS: Checkpoint saved with last_id='{last_id}'")
    except Exception as ex:
        logging.error(f"[Langfuse] FAILURE: {ex}", exc_info=True)
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
                    sender.send_messages(ServiceBusMessage(msg))
                    logging.info(f"[Queue] Message {idx}/{len(messages)} sent.")
    except Exception as e:
        logging.error(f"[Queue] FAILURE: {e}", exc_info=True)
        raise e
    logging.info(f"[Queue] SUCCESS: All {len(messages)} messages sent.")


# ==========================================
# MAIN TASK FUNCTION
# ==========================================

def tabak_data_push(ids_per_message: Optional[int] = None, max_messages_per_run: Optional[int] = None) -> None:
    """Fetch unique Transation_ids from Tabak database and dispatch to Service Bus."""
    
    logging.info("=============================================================")
    logging.info("[Timer Task 2] Starting Tabak Data Push...")
    logging.info("=============================================================")
    
    # ==========================================
    # RUNTIME ENVIRONMENT VALIDATION
    # ==========================================
    logging.info("[Config] Validating environment...")
    
    langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "").strip()
    if not langfuse_environment:
        raise ValueError("LANGFUSE_ENVIRONMENT must be set.")
    
    checkpoint_dataset_name = f"tabak_accuracy_checkpoint_{langfuse_environment}"
    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
    if not queue_name:
        raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")
    
    ids_per_message_str = os.getenv("IDS_PER_MESSAGE", "").strip()
    if not ids_per_message_str:
        raise ValueError("IDS_PER_MESSAGE must be set.")
    eff_ids_per_message = int(ids_per_message_str)
    if ids_per_message is not None:
        eff_ids_per_message = ids_per_message
    
    max_messages_per_run_str = os.getenv("MAX_MESSAGES_PER_RUN", "").strip()
    if not max_messages_per_run_str:
        raise ValueError("MAX_MESSAGES_PER_RUN must be set.")
    eff_max_messages_per_run = int(max_messages_per_run_str)
    if max_messages_per_run is not None:
        eff_max_messages_per_run = max_messages_per_run
    
    logging.info(f"[Config] Environment valid. IDs/msg={eff_ids_per_message}, MaxMsgs={eff_max_messages_per_run}")
    
    # ==========================================
    # STEP 1: Load checkpoint
    # ==========================================
    logging.info("[Timer Task 2] Step 1: Loading checkpoint...")
    last_checkpoint_id = _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
    # ==========================================
    # STEP 2: Build query with JSON filters
    # ==========================================
    logging.info("[Timer Task 2] Step 2: Building query...")
    
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
    if last_checkpoint_id:
        logging.info(f"[Timer Task 2] Incremental: last_checkpoint_id='{last_checkpoint_id}'")
        filters.append("Transation_id > %s")
        query_params.append(last_checkpoint_id)
        logging.info(f"[Timer Task 2] Applied checkpoint filter")
    else:
        logging.info("[Timer Task 2] Full dataset query (no checkpoint)")
    
    where_clause = " AND ".join(filters)
    logging.info(f"[Timer Task 2] WHERE clause: {where_clause[:100]}...")
    
    query = (
        "SELECT DISTINCT Transation_id "
        "FROM `Transactions` "
        f"WHERE {where_clause} "
        "ORDER BY Transation_id ASC;"
    )
    
    # ==========================================
    # STEP 3: Fetch from database
    # ==========================================
    logging.info("[Timer Task 2] Step 3: Executing database query...")
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
            unique_keys = [str(row[0]).strip() for row in rows if row[0] is not None]
            logging.info(f"[Timer Task 2] Retrieved {len(unique_keys)} unique IDs.")
    except Exception as e:
        logging.error(f"[Timer Task 2] FAILURE: Database query failed: {e}", exc_info=True)
        raise e
    
    # ==========================================
    # STEP 4: Check if empty
    # ==========================================
    if len(unique_keys) == 0:
        logging.info("[Timer Task 2] No new records. Done.")
        return
    
    # ==========================================
    # STEP 5: Batch IDs
    # ==========================================
    logging.info("[Timer Task 2] Step 5: Batching IDs...")
    chunks = [unique_keys[i:i + eff_ids_per_message] for i in range(0, len(unique_keys), eff_ids_per_message)]
    
    is_capped = False
    if len(chunks) > eff_max_messages_per_run:
        logging.warning(f"[Timer Task 2] CAPPING: {len(chunks)} chunks > {eff_max_messages_per_run} max")
        chunks = chunks[:eff_max_messages_per_run]
        is_capped = True
    
    logging.info(f"[Timer Task 2] Batched into {len(chunks)} message(s).")
    
    # ==========================================
    # STEP 6: Format messages
    # ==========================================
    logging.info("[Timer Task 2] Step 6: Formatting messages...")
    formatted_messages = []
    for chunk in chunks:
        payload = {
            "record_ids": chunk,
            "source": "tabak",
            "environment": langfuse_environment,
            "process_type": "Accuracy",
            "queued_at": datetime.now(timezone.utc).isoformat()
        }
        formatted_messages.append(json.dumps(payload))
    
    last_dispatched_id = chunks[-1][-1]
    logging.info(f"[Timer Task 2] Last ID: '{last_dispatched_id}'")
    
    # ==========================================
    # STEP 7: Dispatch to queue
    # ==========================================
    logging.info(f"[Timer Task 2] Step 7: Dispatching {len(formatted_messages)} message(s)...")
    try:
        _send_to_azure_queue(queue_name, formatted_messages)
    except Exception as e:
        logging.error(f"[Timer Task 2] FAILURE: Queue dispatch failed: {e}", exc_info=True)
        raise e
    
    # ==========================================
    # STEP 8: Save checkpoint
    # ==========================================
    logging.info("[Timer Task 2] Step 8: Saving checkpoint...")
    if is_capped:
        logging.info(f"[Timer Task 2] Capped run - checkpoint: '{last_dispatched_id}'")
    
    try:
        _save_checkpoint_to_langfuse(checkpoint_dataset_name, langfuse_environment, last_dispatched_id)
    except Exception as e:
        logging.error(f"[Timer Task 2] FAILURE: Checkpoint save failed: {e}", exc_info=True)
        raise e
    
    logging.info("=============================================================")
    logging.info("[Timer Task 2] SUCCESS: Tabak workflow completed.")
    logging.info("=============================================================")


if __name__ == "__main__":
    logging.info("Running Tabak data push manually...")
    try:
        tabak_data_push()
    except Exception as main_err:
        logging.critical(f"FATAL: {main_err}", exc_info=True)
        raise main_err