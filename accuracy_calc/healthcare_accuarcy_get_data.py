

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
    
#     if not all([server, port, database, user, password]):
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
#                     "process_type": "Accuracy",
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
#                     "process_type": "Accuracy",
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
import pathlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
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


# ==========================================
# REUSABLE EVALUATION HELPERS
# ==========================================

def flatten_dict(d: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Helper function to flatten nested dictionary configurations [1]."""
    items = []
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_dict(v, new_key, sep=sep).items())
    elif isinstance(d, list):
        for i, v in enumerate(d):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.extend(flatten_dict(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, d))
    return dict(items)


def find_key_recursive(d: Any, target_key: str) -> Optional[Any]:
    """Recursively searches a dictionary structure for a given key [1]."""
    if not isinstance(d, (dict, list)):
        return None
    if isinstance(d, dict):
        if target_key in d:
            return d[target_key]
        for v in d.values():
            res = find_key_recursive(v, target_key)
            if res is not None:
                return res
    elif isinstance(d, list):
        for item in d:
            res = find_key_recursive(item, target_key)
            if res is not None:
                return res
    return None


def find_key_recursive_ci(d: Any, target_key: str) -> Optional[Any]:
    """Recursively searches a dictionary structure for a key (case-insensitive)."""
    if not isinstance(d, (dict, list)):
        return None

    target = target_key.lower()
    if isinstance(d, dict):
        for k, v in d.items():
            if str(k).lower() == target:
                return v
        for v in d.values():
            res = find_key_recursive_ci(v, target_key)
            if res is not None:
                return res
    else:
        for item in d:
            res = find_key_recursive_ci(item, target_key)
            if res is not None:
                return res
    return None


def unwrap_value(val: Any) -> Any:
    """Unwrap OCR-style value blocks like {'value': '...'} to raw values."""
    if isinstance(val, dict) and "value" in val:
        return val.get("value")
    return val


def _to_compare_string(val: Any) -> str:
    """Normalizes values for robust equality comparison."""
    val = unwrap_value(val)
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, sort_keys=True, ensure_ascii=True).strip().lower()
        except Exception:
            return str(val).strip().lower()
    return str(val).strip().lower()


def collect_user_adjusted_pairs(obj: Any, parent_path: str = "") -> List[Dict[str, Any]]:
    """Collects (predicted, user-adjusted) pairs from Allocation-style JSON.

    Pattern handled: keys like User_Status compared against sibling Status.
    """
    pairs: List[Dict[str, Any]] = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith("User_"):
                base_key = k[len("User_"):]
                if base_key in obj:
                    path_prefix = f"{parent_path}." if parent_path else ""
                    pair_key = f"{path_prefix}{k}"
                    pairs.append(
                        {
                            "key": pair_key,
                            "predicted": unwrap_value(obj.get(base_key)),
                            "ground_truth": unwrap_value(v),
                        }
                    )

        for k, v in obj.items():
            next_path = f"{parent_path}.{k}" if parent_path else str(k)
            pairs.extend(collect_user_adjusted_pairs(v, next_path))

    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            next_path = f"{parent_path}[{idx}]"
            pairs.extend(collect_user_adjusted_pairs(item, next_path))

    return pairs


def evaluate_healthcare_record(raw_json_str: str) -> Dict[str, Any]:
    """
    Parses EOB or Superbill raw JSON payloads, flattens comparison blocks,
    and returns matches, mismatches, and overall accuracy percentages [1].
    """
    result = {
        "filename": "",
        "client_name": "",
        "rawjson": raw_json_str,
        "ground_truth": {},
        "total_matches": 0,
        "total_mismatches": 0,
        "accuracy_percentage": 0.0
    }
    
    if not raw_json_str:
        return result
        
    try:
        data = json.loads(raw_json_str)
    except Exception:
        return result

    # 1. Resolve filename recursively
    for key in ["File_name", "filename", "file_name", "file_path", "File_url", "original_file", "OriginalFile"]:
        val = find_key_recursive_ci(data, key)
        val = unwrap_value(val)
        if val:
            val_str = str(val).replace("\\", "/")
            result["filename"] = pathlib.Path(val_str).name
            break

    # 2. Resolve client_name recursively
    for key in ["Client", "client_name", "client", "clientName", "provider", "provider_name", "ProviderName", "facility"]:
        val = find_key_recursive_ci(data, key)
        val = unwrap_value(val)
        if val:
            result["client_name"] = str(val).strip()
            break

    # 3. Resolve generated (prediction) vs user (ground truth) containers [1]
    pred_keys = ["generated_response", "predictions", "prediction", "extracted_data", "ai_extracted"]
    gt_keys = ["user_selected_response", "ground_truth", "audited_data", "user_corrected", "final_data", "audited"]
    
    pred_dict = {}
    gt_dict = {}
    
    for k in pred_keys:
        if k in data:
            pred_dict = data[k]
            break
    for k in gt_keys:
        if k in data:
            gt_dict = data[k]
            break

    # Fallback to recursive search if top-level containers are not matched
    if not pred_dict:
        for k in pred_keys:
            found = find_key_recursive_ci(data, k)
            if isinstance(found, dict):
                pred_dict = found
                break
    if not gt_dict:
        for k in gt_keys:
            found = find_key_recursive_ci(data, k)
            if isinstance(found, dict):
                gt_dict = found
                break

    if pred_dict and gt_dict:
        flat_pred = flatten_dict(pred_dict)
        flat_gt = flatten_dict(gt_dict)
        
        result["ground_truth"] = flat_gt
        
        matches = 0
        mismatches = 0
        
        for key, gt_val in flat_gt.items():
            pred_val = flat_pred.get(key)
            
            gt_str = str(gt_val).strip().lower() if gt_val is not None else ""
            pred_str = str(pred_val).strip().lower() if pred_val is not None else ""
            
            if gt_str == pred_str:
                matches += 1
            else:
                mismatches += 1
                
        total_fields = matches + mismatches
        accuracy = (matches / total_fields * 100.0) if total_fields > 0 else 0.0
        
        result["total_matches"] = matches
        result["total_mismatches"] = mismatches
        result["accuracy_percentage"] = round(accuracy, 2)

    else:
        # Fallback mode for Allocation-style payloads where user edits are captured as
        # sibling fields (e.g., User_Status vs Status) in the same raw JSON structure.
        adjusted_pairs = collect_user_adjusted_pairs(data)
        if adjusted_pairs:
            gt = {p["key"]: p["ground_truth"] for p in adjusted_pairs}
            matches = 0
            mismatches = 0

            for pair in adjusted_pairs:
                pred_str = _to_compare_string(pair["predicted"])
                gt_str = _to_compare_string(pair["ground_truth"])
                if pred_str == gt_str:
                    matches += 1
                else:
                    mismatches += 1

            total_fields = matches + mismatches
            accuracy = (matches / total_fields * 100.0) if total_fields > 0 else 0.0

            result["ground_truth"] = gt
            result["total_matches"] = matches
            result["total_mismatches"] = mismatches
            result["accuracy_percentage"] = round(accuracy, 2)
        
    return result


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
        
        checkpoint_item_id = f"{checkpoint_dataset_name}::checkpoint::id::{last_id}"
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


def _save_predictions_to_langfuse(predictions_dataset_name: str, langfuse_environment: str, source_name: str, records: List[Dict[str, Any]]) -> None:
    """Saves evaluated prediction payload matrices directly to Langfuse [1]."""
    logging.info(f"[Langfuse] Saving {len(records)} prediction items to dataset '{predictions_dataset_name}'...")
    try:
        langfuse = _get_langfuse_client()
    except Exception as init_err:
        logging.error(f"[Langfuse] Cannot connect to Langfuse client: {init_err}")
        raise init_err

    try:
        langfuse.create_dataset(
            name=predictions_dataset_name,
            description=f"Healthcare {source_name} accuracy predictions ({langfuse_environment})"
        )

        for record in records:
            record_id = record["id"]
            
            payload = {
                "id": record_id,
                "filename": record["filename"],
                "client_name": record["client_name"],
                "rawjson": record["rawjson"],
                "ground_truth": record["ground_truth"],
                "total_matches": record["total_matches"],
                "total_mismatches": record["total_mismatches"],
                "accuracy_percentage": record["accuracy_percentage"]
            }

            logging.debug(f"[Langfuse] Uploading prediction item for record_id '{record_id}' to dataset '{predictions_dataset_name}'...")
            langfuse.create_dataset_item(
                dataset_name=predictions_dataset_name,
                input=payload,
                metadata={
                    "record_type": "healthcare_prediction",
                    "record_id": record_id,
                    "source": source_name,
                    "environment": langfuse_environment,
                    "saved_at": datetime.now(timezone.utc).isoformat()
                }
            )

        langfuse.flush()
        logging.info(f"[Langfuse] SUCCESS: Saved {len(records)} items to dataset '{predictions_dataset_name}'")
    except Exception as ex:
        logging.error(f"[Langfuse] FAILURE: Error updating predictions dataset: {ex}", exc_info=True)
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

    # Predictions Dataset Toggle & Names Setup [1]
    save_predictions_dataset_str = os.getenv("SAVE_PREDICTIONS_DATASET", "TRUE").strip().upper()
    save_predictions_dataset = save_predictions_dataset_str in ("TRUE", "1", "YES")
    eob_predictions_dataset = f"healthcare_predictions_eob_{langfuse_environment}"
    sb_predictions_dataset = f"healthcare_predictions_superbill_{langfuse_environment}"
    logging.info(f"[Config] Save Predictions Dataset Enabled: {save_predictions_dataset}")
    
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
            "SELECT DISTINCT Id, rawJson "
            "FROM `EOBAllocations` "
            f"WHERE {eob_where_clause} "
            "ORDER BY Id ASC;"
        )
        
        logging.info("[Timer Task 3] Step 3: Executing EOB database query...")
        try:
            cursor = conn.cursor()
            cursor.execute(eob_query, eob_query_params)
            eob_rows = cursor.fetchall()
            
            eob_records = []
            for row in eob_rows:
                if row[0] is not None:
                    record_id = str(row[0]).strip()
                    raw_json_str = str(row[1]).strip() if row[1] is not None else ""
                    
                    # Evaluate accuracy metrics [1]
                    eval_metrics = evaluate_healthcare_record(raw_json_str)
                    
                    eob_records.append({
                        "id": record_id,
                        **eval_metrics
                    })
                    
            logging.info(f"[Timer Task 3] Retrieved {len(eob_records)} EOB records.")
        except Exception as e:
            logging.error(f"[Timer Task 3] EOB query failed: {e}", exc_info=True)
            raise e
        
        if eob_records:
            logging.info("[Timer Task 3] Step 4: Batching EOB records...")
            eob_chunks = [eob_records[i:i + eff_ids_per_message] for i in range(0, len(eob_records), eff_ids_per_message)]
            
            is_eob_capped = False
            if len(eob_chunks) > eff_max_messages_per_run:
                logging.warning(f"[Timer Task 3] EOB CAPPED: {len(eob_chunks)} chunks > {eff_max_messages_per_run} max")
                eob_chunks = eob_chunks[:eff_max_messages_per_run]
                is_eob_capped = True
            
            logging.info(f"[Timer Task 3] Batched into {len(eob_chunks)} message(s).")
            
            logging.info("[Timer Task 3] Step 5: Formatting EOB messages...")
            eob_messages = []
            for chunk in eob_chunks:
                chunk_ids = [r["id"] for r in chunk]
                payload = {
                    "record_ids": chunk_ids,
                    "source": "healthcare_eob",
                    "environment": langfuse_environment,
                    "process_type": "Accuracy",
                    "queued_at": datetime.now(timezone.utc).isoformat()
                }
                eob_messages.append(json.dumps(payload))
            
            eob_last_id = eob_chunks[-1][-1]["id"]
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
                logging.warning(f"[Timer Task 3] EOB checkpoint save skipped due to Langfuse error: {e}", exc_info=True)

            # Save Predictions Dataset (Dataset #2 for EOB) - Triggered via Toggle [1]
            if save_predictions_dataset:
                dispatched_eob = [rec for chunk in eob_chunks for rec in chunk]
                logging.info(f"[Timer Task 3] Saving {len(dispatched_eob)} EOB items to predictions dataset '{eob_predictions_dataset}'...")
                try:
                    _save_predictions_to_langfuse(eob_predictions_dataset, langfuse_environment, "EOB", dispatched_eob)
                except Exception as e:
                    logging.warning(f"[Timer Task 3] EOB predictions dataset save skipped due to Langfuse error: {e}", exc_info=True)
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
            "SELECT DISTINCT Id, RawJson "
            "FROM `SuperBillAllocations` "
            f"WHERE {sb_where_clause} "
            "ORDER BY Id ASC;"
        )
        
        logging.info("[Timer Task 3] Step 3: Executing Superbill database query...")
        try:
            cursor.execute(sb_query, sb_query_params)
            sb_rows = cursor.fetchall()
            
            sb_records = []
            for row in sb_rows:
                if row[0] is not None:
                    record_id = str(row[0]).strip()
                    raw_json_str = str(row[1]).strip() if row[1] is not None else ""
                    
                    # Evaluate accuracy metrics [1]
                    eval_metrics = evaluate_healthcare_record(raw_json_str)
                    
                    sb_records.append({
                        "id": record_id,
                        **eval_metrics
                    })
                    
            cursor.close()
            logging.info(f"[Timer Task 3] Retrieved {len(sb_records)} Superbill records.")
        except Exception as e:
            logging.error(f"[Timer Task 3] Superbill query failed: {e}", exc_info=True)
            raise e
        
        if sb_records:
            logging.info("[Timer Task 3] Step 4: Batching Superbill records...")
            sb_chunks = [sb_records[i:i + eff_ids_per_message] for i in range(0, len(sb_records), eff_ids_per_message)]
            
            is_sb_capped = False
            if len(sb_chunks) > eff_max_messages_per_run:
                logging.warning(f"[Timer Task 3] Superbill CAPPED: {len(sb_chunks)} chunks > {eff_max_messages_per_run} max")
                sb_chunks = sb_chunks[:eff_max_messages_per_run]
                is_sb_capped = True
            
            logging.info(f"[Timer Task 3] Batched into {len(sb_chunks)} message(s).")
            
            logging.info("[Timer Task 3] Step 5: Formatting Superbill messages...")
            sb_messages = []
            for chunk in sb_chunks:
                chunk_ids = [r["id"] for r in chunk]
                payload = {
                    "record_ids": chunk_ids,
                    "source": "healthcare_superbill",
                    "environment": langfuse_environment,
                    "process_type": "Accuracy",
                    "queued_at": datetime.now(timezone.utc).isoformat()
                }
                sb_messages.append(json.dumps(payload))
            
            sb_last_id = sb_chunks[-1][-1]["id"]
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
                logging.warning(f"[Timer Task 3] Superbill checkpoint save skipped due to Langfuse error: {e}", exc_info=True)

            # Save Predictions Dataset (Dataset #2 for Superbill) - Triggered via Toggle [1]
            if save_predictions_dataset:
                dispatched_sb = [rec for chunk in sb_chunks for rec in chunk]
                logging.info(f"[Timer Task 3] Saving {len(dispatched_sb)} Superbill items to predictions dataset '{sb_predictions_dataset}'...")
                try:
                    _save_predictions_to_langfuse(sb_predictions_dataset, langfuse_environment, "Superbill", dispatched_sb)
                except Exception as e:
                    logging.warning(f"[Timer Task 3] Superbill predictions dataset save skipped due to Langfuse error: {e}", exc_info=True)
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