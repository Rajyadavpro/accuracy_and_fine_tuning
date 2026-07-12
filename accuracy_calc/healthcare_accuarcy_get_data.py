# import os
# import pymysql
# import logging
# import json
# import tempfile
# from datetime import datetime, timezone
# from typing import Optional, List, Tuple
# from azure.servicebus import ServiceBusClient, ServiceBusMessage

# # ==========================================
# # SETUP DUAL-LOGGING (CONSOLE & FILE)
# # ==========================================

# DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "healthcare_data_push.log")
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


# def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, source_name: str, last_id: str) -> None:
#     """Save last processed ID to Langfuse."""
#     logging.info(f"[Langfuse] Saving checkpoint for {source_name}: last_id='{last_id}'...")
#     try:
#         langfuse = _get_langfuse_client()
#     except Exception as init_err:
#         logging.error(f"[Langfuse] Cannot save checkpoint: {init_err}")
#         raise init_err

#     try:
#         langfuse.create_dataset(
#             name=checkpoint_dataset_name,
#             description=f"Healthcare {source_name} accuracy checkpoint ({langfuse_environment})"
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
#             metadata={"record_type": "checkpoint", "source": source_name, "last_id": last_id}
#         )
        
#         langfuse.flush()
#         logging.info(f"[Langfuse] SUCCESS: {source_name} checkpoint saved with last_id='{last_id}'")
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
#         raise ValueError("Missing database credentials: HEALTHCARE_AI_DB_SERVER, HEALTHCARE_AI_DB_DATABASE, HEALTHCARE_AI_DB_USERID, HEALTHCARE_AI_DB_PASSWORD")

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

# def healthcare_data_push(ids_per_message: Optional[int] = None, max_messages_per_run: Optional[int] = None) -> None:
#     """Fetch healthcare records (EOB & Superbill) and dispatch to Service Bus."""
    
#     logging.info("=============================================================")
#     logging.info("[Timer Task 3] Starting Healthcare Data Push (EOB & Superbill)...")
#     logging.info("=============================================================")
    
#     # ==========================================
#     # RUNTIME ENVIRONMENT VALIDATION
#     # ==========================================
#     logging.info("[Config] Validating environment...")
    
#     langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "").strip()
#     if not langfuse_environment:
#         raise ValueError("LANGFUSE_ENVIRONMENT must be set.")
    
#     eob_checkpoint_dataset = f"healthcare_accuracy_eob_{langfuse_environment}"
#     sb_checkpoint_dataset = f"healthcare_accuracy_superbill_{langfuse_environment}"
    
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
#     # DATABASE CONNECTION
#     # ==========================================
#     server = os.getenv("HEALTHCARE_AI_DB_SERVER", "").strip()
#     port = os.getenv("HEALTHCARE_AI_DB_PORT", "").strip()
#     database = os.getenv("HEALTHCARE_AI_DB_DATABASE", "").strip()
#     user = os.getenv("HEALTHCARE_AI_DB_USERID", "").strip()
#     password = os.getenv("HEALTHCARE_AI_DB_PASSWORD", "").strip()
    
#     if not all([server, port, database, user, password]):
#         raise ValueError("Missing database configuration: HEALTHCARE_AI_DB_SERVER, HEALTHCARE_AI_DB_PORT, HEALTHCARE_AI_DB_DATABASE, HEALTHCARE_AI_DB_USERID, HEALTHCARE_AI_DB_PASSWORD")
    
#     try:
#         conn = _get_db_connection(server, port, database, user, password)
#     except Exception as e:
#         logging.error(f"[Timer Task 3] Cannot connect to database: {e}", exc_info=True)
#         raise e
    
#     try:
#         # ==========================================
#         # PROCESS EOB RECORDS
#         # ==========================================
#         logging.info("[Timer Task 3] ===== Processing EOB Records =====")
#         logging.info("[Timer Task 3] Step 1: Loading EOB checkpoint...")
#         eob_checkpoint_id = _load_checkpoint_from_langfuse(eob_checkpoint_dataset)
        
#         logging.info("[Timer Task 3] Step 2: Building EOB query with JSON filters...")
#         eob_filters = [
#             "Id IS NOT NULL",
#             "rawJson IS NOT NULL",
#             "rawJson != ''",
#         ]
        
#         eob_query_params = []
#         if eob_checkpoint_id:
#             logging.info(f"[Timer Task 3] Incremental EOB: checkpoint_id='{eob_checkpoint_id}'")
#             eob_filters.append("Id > %s")
#             eob_query_params.append(eob_checkpoint_id)
#         else:
#             logging.info("[Timer Task 3] Full EOB dataset query (no checkpoint)")
        
#         eob_where_clause = " AND ".join(eob_filters)
#         eob_query = (
#             "SELECT DISTINCT Id "
#             "FROM `EOBAllocations` "
#             f"WHERE {eob_where_clause} "
#             "ORDER BY Id ASC;"
#         )
        
#         logging.info("[Timer Task 3] Step 3: Executing EOB database query...")
#         try:
#             cursor = conn.cursor()
#             cursor.execute(eob_query, eob_query_params)
#             eob_rows = cursor.fetchall()
#             eob_ids = [str(row[0]).strip() for row in eob_rows if row[0] is not None]
#             logging.info(f"[Timer Task 3] Retrieved {len(eob_ids)} EOB IDs.")
#         except Exception as e:
#             logging.error(f"[Timer Task 3] EOB query failed: {e}", exc_info=True)
#             raise e
        
#         if eob_ids:
#             logging.info("[Timer Task 3] Step 4: Batching EOB records...")
#             eob_chunks = [eob_ids[i:i + eff_ids_per_message] for i in range(0, len(eob_ids), eff_ids_per_message)]
            
#             is_eob_capped = False
#             if len(eob_chunks) > eff_max_messages_per_run:
#                 logging.warning(f"[Timer Task 3] EOB CAPPED: {len(eob_chunks)} chunks > {eff_max_messages_per_run} max")
#                 eob_chunks = eob_chunks[:eff_max_messages_per_run]
#                 is_eob_capped = True
            
#             logging.info(f"[Timer Task 3] Batched into {len(eob_chunks)} message(s).")
            
#             logging.info("[Timer Task 3] Step 5: Formatting EOB messages...")
#             eob_messages = []
#             for chunk in eob_chunks:
#                 payload = {
#                     "record_ids": chunk,
#                     "source": "healthcare_eob",
#                     "environment": langfuse_environment,
#                     "queued_at": datetime.now(timezone.utc).isoformat()
#                 }
#                 eob_messages.append(json.dumps(payload))
            
#             eob_last_id = eob_chunks[-1][-1]
#             logging.info(f"[Timer Task 3] EOB Last ID: '{eob_last_id}'")
            
#             logging.info(f"[Timer Task 3] Step 6: Dispatching {len(eob_messages)} EOB message(s)...")
#             try:
#                 _send_to_azure_queue(queue_name, eob_messages)
#             except Exception as e:
#                 logging.error(f"[Timer Task 3] EOB dispatch failed: {e}", exc_info=True)
#                 raise e
            
#             logging.info("[Timer Task 3] Step 7: Saving EOB checkpoint...")
#             try:
#                 _save_checkpoint_to_langfuse(eob_checkpoint_dataset, langfuse_environment, "EOB", eob_last_id)
#             except Exception as e:
#                 logging.error(f"[Timer Task 3] EOB checkpoint save failed: {e}", exc_info=True)
#                 raise e
#         else:
#             logging.info("[Timer Task 3] No new EOB records.")
        
#         # ==========================================
#         # PROCESS SUPERBILL RECORDS
#         # ==========================================
#         logging.info("[Timer Task 3] ===== Processing Superbill Records =====")
#         logging.info("[Timer Task 3] Step 1: Loading Superbill checkpoint...")
#         sb_checkpoint_id = _load_checkpoint_from_langfuse(sb_checkpoint_dataset)
        
#         logging.info("[Timer Task 3] Step 2: Building Superbill query with JSON filters...")
#         sb_filters = [
#             "Id IS NOT NULL",
#             "RawJson IS NOT NULL",
#             "RawJson != ''",
#         ]
        
#         sb_query_params = []
#         if sb_checkpoint_id:
#             logging.info(f"[Timer Task 3] Incremental Superbill: checkpoint_id='{sb_checkpoint_id}'")
#             sb_filters.append("Id > %s")
#             sb_query_params.append(sb_checkpoint_id)
#         else:
#             logging.info("[Timer Task 3] Full Superbill dataset query (no checkpoint)")
        
#         sb_where_clause = " AND ".join(sb_filters)
#         sb_query = (
#             "SELECT DISTINCT Id "
#             "FROM `SuperBillAllocations` "
#             f"WHERE {sb_where_clause} "
#             "ORDER BY Id ASC;"
#         )
        
#         logging.info("[Timer Task 3] Step 3: Executing Superbill database query...")
#         try:
#             cursor.execute(sb_query, sb_query_params)
#             sb_rows = cursor.fetchall()
#             sb_ids = [str(row[0]).strip() for row in sb_rows if row[0] is not None]
#             cursor.close()
#             logging.info(f"[Timer Task 3] Retrieved {len(sb_ids)} Superbill IDs.")
#         except Exception as e:
#             logging.error(f"[Timer Task 3] Superbill query failed: {e}", exc_info=True)
#             raise e
        
#         if sb_ids:
#             logging.info("[Timer Task 3] Step 4: Batching Superbill records...")
#             sb_chunks = [sb_ids[i:i + eff_ids_per_message] for i in range(0, len(sb_ids), eff_ids_per_message)]
            
#             is_sb_capped = False
#             if len(sb_chunks) > eff_max_messages_per_run:
#                 logging.warning(f"[Timer Task 3] Superbill CAPPED: {len(sb_chunks)} chunks > {eff_max_messages_per_run} max")
#                 sb_chunks = sb_chunks[:eff_max_messages_per_run]
#                 is_sb_capped = True
            
#             logging.info(f"[Timer Task 3] Batched into {len(sb_chunks)} message(s).")
            
#             logging.info("[Timer Task 3] Step 5: Formatting Superbill messages...")
#             sb_messages = []
#             for chunk in sb_chunks:
#                 payload = {
#                     "record_ids": chunk,
#                     "source": "healthcare_superbill",
#                     "environment": langfuse_environment,
#                     "queued_at": datetime.now(timezone.utc).isoformat()
#                 }
#                 sb_messages.append(json.dumps(payload))
            
#             sb_last_id = sb_chunks[-1][-1]
#             logging.info(f"[Timer Task 3] Superbill Last ID: '{sb_last_id}'")
            
#             logging.info(f"[Timer Task 3] Step 6: Dispatching {len(sb_messages)} Superbill message(s)...")
#             try:
#                 _send_to_azure_queue(queue_name, sb_messages)
#             except Exception as e:
#                 logging.error(f"[Timer Task 3] Superbill dispatch failed: {e}", exc_info=True)
#                 raise e
            
#             logging.info("[Timer Task 3] Step 7: Saving Superbill checkpoint...")
#             try:
#                 _save_checkpoint_to_langfuse(sb_checkpoint_dataset, langfuse_environment, "Superbill", sb_last_id)
#             except Exception as e:
#                 logging.error(f"[Timer Task 3] Superbill checkpoint save failed: {e}", exc_info=True)
#                 raise e
#         else:
#             logging.info("[Timer Task 3] No new Superbill records.")
        
#         logging.info("=============================================================")
#         logging.info("[Timer Task 3] SUCCESS: Healthcare workflow completed.")
#         logging.info("=============================================================")
        
#     except Exception as e:
#         logging.error(f"[Timer Task 3] FAILURE: {e}", exc_info=True)
#         raise e
#     finally:
#         try:
#             conn.close()
#         except Exception:
#             pass


# if __name__ == "__main__":
#     logging.info("Running Healthcare data push manually...")
#     try:
#         healthcare_data_push()
#     except Exception as main_err:
#         logging.critical(f"FATAL: {main_err}", exc_info=True)
#         raise main_err


import os
import pymysql
import logging
import json
import tempfile
from datetime import datetime, timezone
from typing import Optional, List, Tuple
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ==========================================
# SETUP DUAL-LOGGING (CONSOLE & FILE)
# ==========================================

DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "healthcare_data_push.log")
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


def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, langfuse_environment: str, source_name: str, last_id: str) -> None:
    """Save last processed ID to Langfuse."""
    logging.info(f"[Langfuse] Saving checkpoint for {source_name}: last_id='{last_id}'...")
    try:
        langfuse = _get_langfuse_client()
    except Exception as init_err:
        logging.error(f"[Langfuse] Cannot save checkpoint: {init_err}")
        raise init_err

    try:
        langfuse.create_dataset(
            name=checkpoint_dataset_name,
            description=f"Healthcare {source_name} accuracy checkpoint ({langfuse_environment})"
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
            metadata={"record_type": "checkpoint", "source": source_name, "last_id": last_id}
        )
        
        langfuse.flush()
        logging.info(f"[Langfuse] SUCCESS: {source_name} checkpoint saved with last_id='{last_id}'")
    except Exception as ex:
        logging.error(f"[Langfuse] FAILURE: {ex}", exc_info=True)
        raise ex


# ==========================================
# DATABASE UTILITIES
# ==========================================

def _get_db_connection(server: str, port: str, database: str, user: str, password: str):
    """Create MySQL/MariaDB connection."""
    logging.info(f"[Database] Connecting to {server}:{port}/{database}...")
    
    if not all([server, port, database, user, password]):
        raise ValueError("Missing database credentials: HEALTHCARE_AI_DB_SERVER, HEALTHCARE_AI_DB_DATABASE, HEALTHCARE_AI_DB_USERID, HEALTHCARE_AI_DB_PASSWORD")

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

def healthcare_data_push(ids_per_message: Optional[int] = None, max_messages_per_run: Optional[int] = None) -> None:
    """Fetch healthcare records (EOB & Superbill) and dispatch to Service Bus."""
    
    logging.info("=============================================================")
    logging.info("[Timer Task 3] Starting Healthcare Data Push (EOB & Superbill)...")
    logging.info("=============================================================")
    
    # ==========================================
    # RUNTIME ENVIRONMENT VALIDATION
    # ==========================================
    logging.info("[Config] Validating environment...")
    
    langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "").strip()
    if not langfuse_environment:
        raise ValueError("LANGFUSE_ENVIRONMENT must be set.")
    
    eob_checkpoint_dataset = f"healthcare_accuracy_eob_{langfuse_environment}"
    sb_checkpoint_dataset = f"healthcare_accuracy_superbill_{langfuse_environment}"
    
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
    # DATABASE CONNECTION
    # ==========================================
    server = os.getenv("HEALTHCARE_AI_DB_SERVER", "").strip()
    port = os.getenv("HEALTHCARE_AI_DB_PORT", "").strip()
    database = os.getenv("HEALTHCARE_AI_DB_DATABASE", "").strip()
    user = os.getenv("HEALTHCARE_AI_DB_USERID", "").strip()
    password = os.getenv("HEALTHCARE_AI_DB_PASSWORD", "").strip()
    
    if not all([server, port, database, user, password]):
        raise ValueError("Missing database configuration: HEALTHCARE_AI_DB_SERVER, HEALTHCARE_AI_DB_PORT, HEALTHCARE_AI_DB_DATABASE, HEALTHCARE_AI_DB_USERID, HEALTHCARE_AI_DB_PASSWORD")
    
    try:
        conn = _get_db_connection(server, port, database, user, password)
    except Exception as e:
        logging.error(f"[Timer Task 3] Cannot connect to database: {e}", exc_info=True)
        raise e
    
    try:
        # ==========================================
        # PROCESS EOB RECORDS
        # ==========================================
        logging.info("[Timer Task 3] ===== Processing EOB Records =====")
        logging.info("[Timer Task 3] Step 1: Loading EOB checkpoint...")
        eob_checkpoint_id = _load_checkpoint_from_langfuse(eob_checkpoint_dataset)
        
        logging.info("[Timer Task 3] Step 2: Building EOB query with JSON filters...")
        eob_filters = [
            "Id IS NOT NULL",
            "rawJson IS NOT NULL",
            "rawJson != ''",
        ]
        
        eob_query_params = []
        if eob_checkpoint_id:
            logging.info(f"[Timer Task 3] Incremental EOB: checkpoint_id='{eob_checkpoint_id}'")
            eob_filters.append("Id > %s")
            eob_query_params.append(eob_checkpoint_id)
        else:
            logging.info("[Timer Task 3] Full EOB dataset query (no checkpoint)")
        
        eob_where_clause = " AND ".join(eob_filters)
        eob_query = (
            "SELECT DISTINCT Id "
            "FROM `EOBAllocations` "
            f"WHERE {eob_where_clause} "
            "ORDER BY Id ASC;"
        )
        
        logging.info("[Timer Task 3] Step 3: Executing EOB database query...")
        try:
            cursor = conn.cursor()
            cursor.execute(eob_query, eob_query_params)
            eob_rows = cursor.fetchall()
            eob_ids = [str(row[0]).strip() for row in eob_rows if row[0] is not None]
            logging.info(f"[Timer Task 3] Retrieved {len(eob_ids)} EOB IDs.")
        except Exception as e:
            logging.error(f"[Timer Task 3] EOB query failed: {e}", exc_info=True)
            raise e
        
        if eob_ids:
            logging.info("[Timer Task 3] Step 4: Batching EOB records...")
            eob_chunks = [eob_ids[i:i + eff_ids_per_message] for i in range(0, len(eob_ids), eff_ids_per_message)]
            
            is_eob_capped = False
            if len(eob_chunks) > eff_max_messages_per_run:
                logging.warning(f"[Timer Task 3] EOB CAPPED: {len(eob_chunks)} chunks > {eff_max_messages_per_run} max")
                eob_chunks = eob_chunks[:eff_max_messages_per_run]
                is_eob_capped = True
            
            logging.info(f"[Timer Task 3] Batched into {len(eob_chunks)} message(s).")
            
            logging.info("[Timer Task 3] Step 5: Formatting EOB messages...")
            eob_messages = []
            for chunk in eob_chunks:
                payload = {
                    "record_ids": chunk,
                    "source": "healthcare_eob",
                    "environment": langfuse_environment,
                    "process_type": "Accuracy",
                    "queued_at": datetime.now(timezone.utc).isoformat()
                }
                eob_messages.append(json.dumps(payload))
            
            eob_last_id = eob_chunks[-1][-1]
            logging.info(f"[Timer Task 3] EOB Last ID: '{eob_last_id}'")
            
            logging.info(f"[Timer Task 3] Step 6: Dispatching {len(eob_messages)} EOB message(s)...")
            try:
                _send_to_azure_queue(queue_name, eob_messages)
            except Exception as e:
                logging.error(f"[Timer Task 3] EOB dispatch failed: {e}", exc_info=True)
                raise e
            
            logging.info("[Timer Task 3] Step 7: Saving EOB checkpoint...")
            try:
                _save_checkpoint_to_langfuse(eob_checkpoint_dataset, langfuse_environment, "EOB", eob_last_id)
            except Exception as e:
                logging.error(f"[Timer Task 3] EOB checkpoint save failed: {e}", exc_info=True)
                raise e
        else:
            logging.info("[Timer Task 3] No new EOB records.")
        
        # ==========================================
        # PROCESS SUPERBILL RECORDS
        # ==========================================
        logging.info("[Timer Task 3] ===== Processing Superbill Records =====")
        logging.info("[Timer Task 3] Step 1: Loading Superbill checkpoint...")
        sb_checkpoint_id = _load_checkpoint_from_langfuse(sb_checkpoint_dataset)
        
        logging.info("[Timer Task 3] Step 2: Building Superbill query with JSON filters...")
        sb_filters = [
            "Id IS NOT NULL",
            "RawJson IS NOT NULL",
            "RawJson != ''",
        ]
        
        sb_query_params = []
        if sb_checkpoint_id:
            logging.info(f"[Timer Task 3] Incremental Superbill: checkpoint_id='{sb_checkpoint_id}'")
            sb_filters.append("Id > %s")
            sb_query_params.append(sb_checkpoint_id)
        else:
            logging.info("[Timer Task 3] Full Superbill dataset query (no checkpoint)")
        
        sb_where_clause = " AND ".join(sb_filters)
        sb_query = (
            "SELECT DISTINCT Id "
            "FROM `SuperBillAllocations` "
            f"WHERE {sb_where_clause} "
            "ORDER BY Id ASC;"
        )
        
        logging.info("[Timer Task 3] Step 3: Executing Superbill database query...")
        try:
            cursor.execute(sb_query, sb_query_params)
            sb_rows = cursor.fetchall()
            sb_ids = [str(row[0]).strip() for row in sb_rows if row[0] is not None]
            cursor.close()
            logging.info(f"[Timer Task 3] Retrieved {len(sb_ids)} Superbill IDs.")
        except Exception as e:
            logging.error(f"[Timer Task 3] Superbill query failed: {e}", exc_info=True)
            raise e
        
        if sb_ids:
            logging.info("[Timer Task 3] Step 4: Batching Superbill records...")
            sb_chunks = [sb_ids[i:i + eff_ids_per_message] for i in range(0, len(sb_ids), eff_ids_per_message)]
            
            is_sb_capped = False
            if len(sb_chunks) > eff_max_messages_per_run:
                logging.warning(f"[Timer Task 3] Superbill CAPPED: {len(sb_chunks)} chunks > {eff_max_messages_per_run} max")
                sb_chunks = sb_chunks[:eff_max_messages_per_run]
                is_sb_capped = True
            
            logging.info(f"[Timer Task 3] Batched into {len(sb_chunks)} message(s).")
            
            logging.info("[Timer Task 3] Step 5: Formatting Superbill messages...")
            sb_messages = []
            for chunk in sb_chunks:
                payload = {
                    "record_ids": chunk,
                    "source": "healthcare_superbill",
                    "environment": langfuse_environment,
                    "process_type": "Accuracy",
                    "queued_at": datetime.now(timezone.utc).isoformat()
                }
                sb_messages.append(json.dumps(payload))
            
            sb_last_id = sb_chunks[-1][-1]
            logging.info(f"[Timer Task 3] Superbill Last ID: '{sb_last_id}'")
            
            logging.info(f"[Timer Task 3] Step 6: Dispatching {len(sb_messages)} Superbill message(s)...")
            try:
                _send_to_azure_queue(queue_name, sb_messages)
            except Exception as e:
                logging.error(f"[Timer Task 3] Superbill dispatch failed: {e}", exc_info=True)
                raise e
            
            logging.info("[Timer Task 3] Step 7: Saving Superbill checkpoint...")
            try:
                _save_checkpoint_to_langfuse(sb_checkpoint_dataset, langfuse_environment, "Superbill", sb_last_id)
            except Exception as e:
                logging.error(f"[Timer Task 3] Superbill checkpoint save failed: {e}", exc_info=True)
                raise e
        else:
            logging.info("[Timer Task 3] No new Superbill records.")
        
        logging.info("=============================================================")
        logging.info("[Timer Task 3] SUCCESS: Healthcare workflow completed.")
        logging.info("=============================================================")
        
    except Exception as e:
        logging.error(f"[Timer Task 3] FAILURE: {e}", exc_info=True)
        raise e
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    logging.info("Running Healthcare data push manually...")
    try:
        healthcare_data_push()
    except Exception as main_err:
        logging.critical(f"FATAL: {main_err}", exc_info=True)
        raise main_err