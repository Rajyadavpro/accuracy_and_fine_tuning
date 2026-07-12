# import os
# import pymssql
# import logging
# import json
# import tempfile
# from datetime import datetime, timezone
# from typing import Optional, List, Dict, Any
# from azure.servicebus import ServiceBusClient, ServiceBusMessage

# # ==========================================
# # SETUP DUAL-LOGGING (CONSOLE & FILE)
# # ==========================================

# # Resolve file paths allowing fallback to temp directory if write permissions are constrained
# DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "idp_data_push.log")
# LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", DEFAULT_LOG_FILE)
# LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
# LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

# # Prepare root logger configuration
# root_logger = logging.getLogger()
# root_logger.setLevel(LOG_LEVEL)

# # Remove existing handlers to avoid duplicates in certain run environments
# for handler in root_logger.handlers[:]:
#     root_logger.removeHandler(handler)

# # Unified log format (includes line numbers and exact filenames for traceability)
# log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s")

# # 1. Console Handler
# console_handler = logging.StreamHandler()
# console_handler.setLevel(LOG_LEVEL)
# console_handler.setFormatter(log_formatter)
# root_logger.addHandler(console_handler)

# # 2. File Handler (Protected with try-except fallback)
# file_write_success = False
# try:
#     file_handler = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
#     file_handler.setLevel(LOG_LEVEL)
#     file_handler.setFormatter(log_formatter)
#     root_logger.addHandler(file_handler)
#     file_write_success = True
# except Exception as log_ex:
#     logging.error(f"Failed to initialize log file at '{LOG_FILE_PATH}' due to: {log_ex}. Falling back to console-only logging.")

# logging.info("=============================================================")
# logging.info(f"LOGGING INITIALIZED. Operational Level: {logging.getLevelName(LOG_LEVEL)}")
# if file_write_success:
#     logging.info(f"Logs are being recorded locally to: {LOG_FILE_PATH}")
# logging.info("=============================================================")


# # Ensure Langfuse import is attempted
# try:
#     from langfuse import Langfuse
#     logging.info("[Langfuse] Langfuse library detected and imported successfully.")
# except ImportError:
#     logging.error("[Langfuse] FAILURE: Langfuse library is not installed in the current environment.")
#     Langfuse = None


# def _mask_value(val: Optional[str]) -> str:
#     """Masks secret values to safeguard credentials in logs."""
#     if not val:
#         return "Not Set"
#     val = val.strip()
#     if len(val) <= 4:
#         return "****"
#     return f"{val[:2]}...{val[-2:]}"


# # ==========================================
# # STRICT CONFIGURATION (No default values)
# # ==========================================

# logging.info("[Config] Starting environment validation...")

# LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT")
# logging.info(f"[Config] Check -> LANGFUSE_ENVIRONMENT: '{LANGFUSE_ENVIRONMENT}'")
# if not LANGFUSE_ENVIRONMENT:
#     logging.error("[Config] FAILURE: Missing critical environment variable: LANGFUSE_ENVIRONMENT")
#     raise ValueError("LANGFUSE_ENVIRONMENT must be set.")
# LANGFUSE_ENVIRONMENT = LANGFUSE_ENVIRONMENT.strip()

# CHECKPOINT_DATASET_NAME = f"idp_accuracy_checkpoint_{LANGFUSE_ENVIRONMENT}"
# logging.info(f"[Config] Derived Dataset Name: '{CHECKPOINT_DATASET_NAME}'")

# QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")
# logging.info(f"[Config] Check -> SERVICE_BUS_QUEUE_NAME: '{QUEUE_NAME}'")
# if not QUEUE_NAME:
#     logging.error("[Config] FAILURE: Missing critical environment variable: SERVICE_BUS_QUEUE_NAME")
#     raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")

# IDS_PER_MESSAGE_STR = os.getenv("IDS_PER_MESSAGE")
# logging.info(f"[Config] Check -> IDS_PER_MESSAGE Raw String: '{IDS_PER_MESSAGE_STR}'")
# if not IDS_PER_MESSAGE_STR:
#     logging.error("[Config] FAILURE: Missing critical environment variable: IDS_PER_MESSAGE")
#     raise ValueError("IDS_PER_MESSAGE must be set.")
# try:
#     IDS_PER_MESSAGE = int(IDS_PER_MESSAGE_STR)
#     logging.info(f"[Config] Parsed -> IDS_PER_MESSAGE Integer Value: {IDS_PER_MESSAGE}")
# except ValueError as e:
#     logging.error(f"[Config] FAILURE: IDS_PER_MESSAGE is not a valid integer: {IDS_PER_MESSAGE_STR}")
#     raise e

# MAX_MESSAGES_PER_RUN_STR = os.getenv("MAX_MESSAGES_PER_RUN")
# logging.info(f"[Config] Check -> MAX_MESSAGES_PER_RUN Raw String: '{MAX_MESSAGES_PER_RUN_STR}'")
# if not MAX_MESSAGES_PER_RUN_STR:
#     logging.error("[Config] FAILURE: Missing critical environment variable: MAX_MESSAGES_PER_RUN")
#     raise ValueError("MAX_MESSAGES_PER_RUN must be set.")
# try:
#     MAX_MESSAGES_PER_RUN = int(MAX_MESSAGES_PER_RUN_STR)
#     logging.info(f"[Config] Parsed -> MAX_MESSAGES_PER_RUN Integer Value: {MAX_MESSAGES_PER_RUN}")
# except ValueError as e:
#     logging.error(f"[Config] FAILURE: MAX_MESSAGES_PER_RUN is not a valid integer: {MAX_MESSAGES_PER_RUN_STR}")
#     raise e

# logging.info("[Config] Environment validation completed successfully.")


# # ==========================================
# # LANGFUSE CLIENT & CHECKPOINT UTILITIES
# # ==========================================

# def _get_langfuse_client() -> Any:
#     """Retrieves and initializes the Langfuse client strictly. Raises error if components are missing."""
#     logging.info("[Langfuse] Initializing Langfuse client...")
    
#     if not Langfuse:
#         logging.error("[Langfuse] FAILURE: Langfuse dependency is missing.")
#         raise ImportError("Langfuse library is not installed.")

#     public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
#     secret_key = os.getenv("LANGFUSE_SECRET_KEY")
#     host = os.getenv("LANGFUSE_HOST")

#     logging.info(f"[Langfuse] Configuration -> Public Key: {_mask_value(public_key)}, Secret Key: {_mask_value(secret_key)}, Host: '{host}'")

#     if not public_key:
#         logging.error("[Langfuse] FAILURE: LANGFUSE_PUBLIC_KEY environment variable is missing.")
#         raise ValueError("LANGFUSE_PUBLIC_KEY is required.")
#     if not secret_key:
#         logging.error("[Langfuse] FAILURE: LANGFUSE_SECRET_KEY environment variable is missing.")
#         raise ValueError("LANGFUSE_SECRET_KEY is required.")
#     if not host:
#         logging.error("[Langfuse] FAILURE: LANGFUSE_HOST environment variable is missing.")
#         raise ValueError("LANGFUSE_HOST is required.")

#     try:
#         client = Langfuse(
#             public_key=public_key.strip(),
#             secret_key=secret_key.strip(),
#             host=host.strip()
#         )
#         logging.info("[Langfuse] SUCCESS: Client successfully initialized.")
#         return client
#     except Exception as e:
#         logging.error(f"[Langfuse] FAILURE: Client initialization failed: {e}", exc_info=True)
#         raise e


# def _load_checkpoint_from_langfuse() -> Optional[str]:
#     """Retrieves the last processed ID from Langfuse. Returns None if no checkpoint exists."""
#     logging.info(f"[Langfuse] Attempting to retrieve checkpoint from dataset '{CHECKPOINT_DATASET_NAME}'...")
#     try:
#         langfuse = _get_langfuse_client()
#     except Exception as init_err:
#         logging.warning(f"[Langfuse] Non-blocking failure to initialize Langfuse client during checkpoint load: {init_err}. Proceeding with clean start.")
#         return None

#     try:
#         logging.info(f"[Langfuse] Requesting dataset '{CHECKPOINT_DATASET_NAME}' metadata from API...")
#         dataset = langfuse.get_dataset(CHECKPOINT_DATASET_NAME)
#         if not dataset:
#             logging.info(f"[Langfuse] RESULT: Dataset '{CHECKPOINT_DATASET_NAME}' does not exist on Langfuse. Proceeding with clean start.")
#             return None
            
#         if not hasattr(dataset, 'items') or not dataset.items:
#             logging.warning(f"[Langfuse] RESULT: Dataset '{CHECKPOINT_DATASET_NAME}' is empty. Proceeding with clean start.")
#             return None
        
#         logging.info(f"[Langfuse] Found {len(dataset.items)} items in dataset. Filtering for the latest checkpoint...")
#         latest_item = max(dataset.items, key=lambda r: getattr(r, 'created_at'))
#         item_id = getattr(latest_item, 'id', 'N/A')
#         logging.info(f"[Langfuse] Selected newest item ID: '{item_id}' (Created At: {getattr(latest_item, 'created_at', 'N/A')})")
        
#         if latest_item and hasattr(latest_item, 'input'):
#             checkpoint_data = latest_item.input
#             if isinstance(checkpoint_data, dict) and "last_id" in checkpoint_data:
#                 last_id = str(checkpoint_data["last_id"])
#                 logging.info(f"[Langfuse] SUCCESS: Checkpoint retrieved. Last processed ID: '{last_id}'")
#                 return last_id
#             else:
#                 logging.error(f"[Langfuse] FAILURE: Checkpoint payload missing 'last_id'. Payload: {checkpoint_data}")
#         else:
#             logging.error(f"[Langfuse] FAILURE: Newest dataset item (ID='{item_id}') is missing the 'input' attribute.")
#     except Exception as ex:
#         logging.warning(f"[Langfuse] Non-blocking exception retrieving checkpoint: {ex}. Assuming clean start.", exc_info=True)
    
#     return None


# def _save_checkpoint_to_langfuse(last_id: str) -> None:
#     """Saves the last processed record ID to Langfuse as the checkpoint."""
#     logging.info(f"[Langfuse] Initiating checkpoint save for last_id: '{last_id}'...")
#     try:
#         langfuse = _get_langfuse_client()
#     except Exception as init_err:
#         logging.error(f"[Langfuse] FAILURE: Could not connect to Langfuse client to save checkpoint: {init_err}")
#         raise init_err

#     try:
#         logging.info(f"[Langfuse] Ensuring dataset '{CHECKPOINT_DATASET_NAME}' exists or is created...")
#         langfuse.create_dataset(
#             name=CHECKPOINT_DATASET_NAME,
#             description=f"IDP accuracy checkpoint ({LANGFUSE_ENVIRONMENT})"
#         )
        
#         checkpoint_item_id = f"checkpoint::id::{last_id}"
#         checkpoint_payload = {
#             "last_id": last_id,
#             "saved_at": datetime.now(timezone.utc).isoformat(),
#         }
        
#         logging.info(f"[Langfuse] Uploading checkpoint item '{checkpoint_item_id}' to dataset '{CHECKPOINT_DATASET_NAME}'...")
#         langfuse.create_dataset_item(
#             dataset_name=CHECKPOINT_DATASET_NAME,
#             id=checkpoint_item_id,
#             input=checkpoint_payload,
#             metadata={
#                 "record_type": "idp_checkpoint",
#                 "last_id": last_id,
#             }
#         )
        
#         logging.info("[Langfuse] Flushing sync buffer...")
#         langfuse.flush()
#         logging.info(f"[Langfuse] SUCCESS: Checkpoint saved with last_id: '{last_id}'")
#     except Exception as ex:
#         logging.error(f"[Langfuse] FAILURE: Error updating checkpoint: {ex}", exc_info=True)
#         raise ex


# # ==========================================
# # DATABASE UTILITIES
# # ==========================================

# def _get_db_connection() -> pymssql.Connection:
#     """Builds and returns a SQL Server connection via pymssql. Raises exception on invalid config or connection failure."""
#     logging.info("[Database] Initiating database connection setup...")
#     server = os.getenv("IDP_SQL_SERVER")
#     database = os.getenv("IDP_SQL_DATABASE")
#     user = os.getenv("IDP_SQL_USER")
#     password = os.getenv("IDP_SQL_PASSWORD")

#     logging.info(
#         f"[Database] Connection configuration check: "
#         f"Server='{server}', Database='{database}', User='{user}', Password_Set={bool(password)}"
#     )

#     if not server:
#         logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_SERVER is not set.")
#         raise ValueError("IDP_SQL_SERVER is required.")
#     if not database:
#         logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_DATABASE is not set.")
#         raise ValueError("IDP_SQL_DATABASE is required.")
#     if not user:
#         logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_USER is not set.")
#         raise ValueError("IDP_SQL_USER is required.")
#     if not password:
#         logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_PASSWORD is not set.")
#         raise ValueError("IDP_SQL_PASSWORD is required.")

#     # Parse host and port from server string (e.g. "myserver.database.windows.net,1433")
#     host = server.strip()
#     port = 1433
#     if "," in host:
#         host, port_str = host.rsplit(",", 1)
#         try:
#             port = int(port_str.strip())
#         except ValueError:
#             logging.warning(f"[Database] Could not parse port from server string '{server}', defaulting to 1433.")

#     try:
#         logging.info(f"[Database] Attempting pymssql connection to Server: '{host}:{port}', Database: '{database}'...")
#         connection = pymssql.connect(
#             server=host,
#             port=port,
#             user=user,
#             password=password,
#             database=database,
#             login_timeout=30,
#             tds_version="7.4"
#         )
#         logging.info("[Database] SUCCESS: Database connection established successfully.")
#         return connection
#     except Exception as e:
#         logging.error(f"[Database] FAILURE: Failed to connect to SQL Server. Server='{host}:{port}', Database='{database}', User='{user}'. Error: {e}", exc_info=True)
#         raise e


# # ==========================================
# # QUEUE UTILITIES
# # ==========================================

# def _send_to_azure_queue(messages: List[str]) -> None:
#     """Dispatches messages to Azure Service Bus queue."""
#     logging.info(f"[Queue] Preparing to dispatch {len(messages)} messages to Azure Service Bus queue '{QUEUE_NAME}'...")
#     connection_string = os.getenv("SERVICE_BUS_CONNECTION_STRING")
#     if not connection_string:
#         logging.error("[Queue] FAILURE: Missing configuration: SERVICE_BUS_CONNECTION_STRING is not set.")
#         raise ValueError("SERVICE_BUS_CONNECTION_STRING is missing.")

#     try:
#         with ServiceBusClient.from_connection_string(connection_string) as client:
#             with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
#                 logging.info(f"[Queue] Connected to Service Bus queue '{QUEUE_NAME}'.")
#                 for idx, msg in enumerate(messages):
#                     try:
#                         logging.info(f"[Queue] Dispatching message {idx + 1}/{len(messages)} (Length: {len(msg)} characters)...")
#                         sender.send_messages(ServiceBusMessage(msg))
#                         logging.info(f"[Queue] SUCCESS: Message {idx + 1}/{len(messages)} was sent successfully.")
#                     except Exception as e:
#                         logging.error(f"[Queue] FAILURE: Failed to transmit message {idx + 1}. Payload excerpt: '{msg[:200]}...'. Error details: {e}", exc_info=True)
#                         raise e
#     except Exception as e:
#         logging.error(f"[Queue] FAILURE: Service Bus client error: {e}", exc_info=True)
#         raise e
#     logging.info(f"[Queue] SUCCESS: All {len(messages)} messages successfully written to Service Bus queue '{QUEUE_NAME}'.")

# # ==========================================
# # TIMER TASK 1 IMPLEMENTATION
# # ==========================================

# def idp_data_push(ids_per_message: Optional[int] = None, max_messages_per_run: Optional[int] = None) -> None:
#     """Performs strict incremental retrieval and queue dispatch. Falls back to env var defaults when parameters are not supplied."""
#     # Fall back to module-level env var values (used by timer trigger which passes no args)
#     if ids_per_message is None:
#         ids_per_message = IDS_PER_MESSAGE
#     if max_messages_per_run is None:
#         max_messages_per_run = MAX_MESSAGES_PER_RUN

#     logging.info("=============================================================")
#     logging.info("[Timer Task 1] Initiating Workflow Execution...")
#     logging.info("=============================================================")
    
#     run_datetime = datetime.now(timezone.utc)
#     logging.info(f"[Timer Task 1] Workflow configurations -> IDs/Message: {ids_per_message}, Max Messages: {max_messages_per_run}, Exec Time: {run_datetime.isoformat()}")

#     # 1. Check the checkpoint on Langfuse
#     logging.info("[Timer Task 1] Step 1: Checking latest checkpoint from Langfuse...")
#     last_checkpoint_id = _load_checkpoint_from_langfuse()
    
#     # 2. Build the query based on checkpoint availability
#     # ORDER BY v.Id ASC so pagination is consistent and deterministic
#     query_params = []
#     if last_checkpoint_id:
#         logging.info(f"[Timer Task 1] Step 2: Checkpoint retrieved. Last processed ID: '{last_checkpoint_id}'. Executing incremental query.")
#         query = (
#             "SELECT DISTINCT v.Id "
#             "FROM dbo.vw_PdfClassificationTransactionLog v "
#             "WHERE v.Id > %s "
#             "ORDER BY v.Id ASC;"
#         )
#         query_params.append(last_checkpoint_id)
#         logging.info(f"[Timer Task 1] Query formulated with last_checkpoint_id='{last_checkpoint_id}'")
#     else:
#         logging.info("[Timer Task 1] Step 2: No checkpoint found. Initializing query over full dataset.")
#         query = (
#             "SELECT DISTINCT v.Id "
#             "FROM dbo.vw_PdfClassificationTransactionLog v "
#             "ORDER BY v.Id ASC;"
#         )
#         logging.info("[Timer Task 1] Query formulated with no parameters.")

#     # 3. Fetch rows from Database
#     logging.info("[Timer Task 1] Step 3: Executing prepared query on SQL Server...")
#     try:
#         with _get_db_connection() as conn:
#             cursor = conn.cursor()
#             logging.info("[Timer Task 1] Query executed. Extracting results...")
#             cursor.execute(query, query_params)
#             rows = cursor.fetchall()
            
#             unique_keys = [str(row[0]).strip() for row in rows if row[0] is not None]
#             logging.info(f"[Timer Task 1] SUCCESS: Retrieved {len(rows)} raw rows. Extracted {len(unique_keys)} clean unique IDs.")
#     except Exception as e:
#         logging.error(f"[Timer Task 1] FAILURE: Database step failed during query execution: {e}", exc_info=True)
#         raise e

#     # 4. Process and calculate keys to dispatch
#     total_keys = len(unique_keys)
#     logging.info(f"[Timer Task 1] Step 4: Assessing structural segments for {total_keys} unique keys.")

#     if total_keys == 0:
#         logging.info("[Timer Task 1] SUCCESS: No new records identified since last execution. Workflow finished cleanly.")
#         return

#     # Group IDs into chunks
#     chunks = [unique_keys[i:i + ids_per_message] for i in range(0, total_keys, ids_per_message)]
#     total_chunks = len(chunks)
#     logging.info(f"[Timer Task 1] Segregation result: {total_chunks} chunk(s) calculated.")
    
#     is_capped = False
#     if total_chunks > max_messages_per_run:
#         logging.warning(f"[Timer Task 1] CAPPING TRIGGERED: Segment chunks ({total_chunks}) exceeds system cap ({max_messages_per_run}).")
#         chunks = chunks[:max_messages_per_run]
#         is_capped = True
#         logging.warning(f"[Timer Task 1] Workflow has been capped to the top {max_messages_per_run} chunks. Remaining items will be processed in subsequent runs.")
#     else:
#         logging.info(f"[Timer Task 1] Volume within execution threshold. All {total_chunks} chunk(s) are eligible to send.")

#     # Format into JSON payloads
#     logging.info("[Timer Task 1] Generating standard message structures...")
#     formatted_messages = []
#     for idx, chunk in enumerate(chunks):
#         payload = {
#             "record_ids": chunk,
#             "source": "idp",
#             "environment": LANGFUSE_ENVIRONMENT,
#             "queued_at": datetime.now(timezone.utc).isoformat()
#         }
#         json_payload = json.dumps(payload)
#         formatted_messages.append(json_payload)
#         logging.debug(f"[Timer Task 1] Chunk {idx + 1} structured with {len(chunk)} keys.")

#     # Last ID in the last dispatched chunk — used as the checkpoint bookmark
#     last_dispatched_id = chunks[-1][-1]
#     logging.info(f"[Timer Task 1] Last dispatched ID: '{last_dispatched_id}'")

#     # 5. Push formatted payloads to the Queue
#     logging.info(f"[Timer Task 1] Step 5: Commencing dispatch of {len(formatted_messages)} messages to queue '{QUEUE_NAME}'...")
#     try:
#         _send_to_azure_queue(formatted_messages)
#         logging.info("[Timer Task 1] SUCCESS: All queue transfers completed.")
#     except Exception as e:
#         logging.error(f"[Timer Task 1] FAILURE: Queue dispatch failed: {e}", exc_info=True)
#         raise e

#     # 6. Save checkpoint to Langfuse using the last dispatched ID
#     # - Capped run: last ID in the last dispatched chunk (partial progress bookmark)
#     # - Full run: same — last ID of all dispatched records
#     if is_capped:
#         logging.info(f"[Timer Task 1] Step 6: Capped run — saving partial progress checkpoint. Last dispatched ID: '{last_dispatched_id}'")
#     else:
#         logging.info(f"[Timer Task 1] Step 6: Full run — saving checkpoint. Last dispatched ID: '{last_dispatched_id}'")

#     try:
#         _save_checkpoint_to_langfuse(last_dispatched_id)
#         logging.info(f"[Timer Task 1] SUCCESS: Checkpoint saved to Langfuse with last_id: '{last_dispatched_id}'")
#     except Exception as e:
#         logging.error(f"[Timer Task 1] FAILURE: Langfuse checkpoint sync failed: {e}", exc_info=True)
#         raise e

#     logging.info("=============================================================")
#     logging.info("[Timer Task 1] Workflow execution successfully completed.")
#     logging.info("=============================================================")


# if __name__ == "__main__":
#     logging.info("Initiating manual trigger execution...")
#     try:
#         idp_data_push(
#             ids_per_message=IDS_PER_MESSAGE,
#             max_messages_per_run=MAX_MESSAGES_PER_RUN
#         )
#     except Exception as main_err:
#         logging.critical(f"Workflow execution halted by uncaught critical exception: {main_err}", exc_info=True)
#         raise main_err



import os
import pymssql
import logging
import json
import tempfile
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ==========================================
# SETUP DUAL-LOGGING (CONSOLE & FILE)
# ==========================================

# Resolve file paths allowing fallback to temp directory if write permissions are constrained
DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "idp_data_push.log")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", DEFAULT_LOG_FILE)
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

# Prepare root logger configuration
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)

# Remove existing handlers to avoid duplicates in certain run environments
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Unified log format (includes line numbers and exact filenames for traceability)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s")

# 1. Console Handler
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

# 2. File Handler (Protected with try-except fallback)
file_write_success = False
try:
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    file_write_success = True
except Exception as log_ex:
    logging.error(f"Failed to initialize log file at '{LOG_FILE_PATH}' due to: {log_ex}. Falling back to console-only logging.")

logging.info("=============================================================")
logging.info(f"LOGGING INITIALIZED. Operational Level: {logging.getLevelName(LOG_LEVEL)}")
if file_write_success:
    logging.info(f"Logs are being recorded locally to: {LOG_FILE_PATH}")
logging.info("=============================================================")


# Ensure Langfuse import is attempted
try:
    from langfuse import Langfuse
    logging.info("[Langfuse] Langfuse library detected and imported successfully.")
except ImportError:
    logging.error("[Langfuse] FAILURE: Langfuse library is not installed in the current environment.")
    Langfuse = None


def _mask_value(val: Optional[str]) -> str:
    """Masks secret values to safeguard credentials in logs."""
    if not val:
        return "Not Set"
    val = val.strip()
    if len(val) <= 4:
        return "****"
    return f"{val[:2]}...{val[-2:]}"


# ==========================================
# STRICT CONFIGURATION (No default values)
# ==========================================

logging.info("[Config] Starting environment validation...")

LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT")
logging.info(f"[Config] Check -> LANGFUSE_ENVIRONMENT: '{LANGFUSE_ENVIRONMENT}'")
if not LANGFUSE_ENVIRONMENT:
    logging.error("[Config] FAILURE: Missing critical environment variable: LANGFUSE_ENVIRONMENT")
    raise ValueError("LANGFUSE_ENVIRONMENT must be set.")
LANGFUSE_ENVIRONMENT = LANGFUSE_ENVIRONMENT.strip()

CHECKPOINT_DATASET_NAME = f"idp_accuracy_checkpoint_{LANGFUSE_ENVIRONMENT}"
logging.info(f"[Config] Derived Dataset Name: '{CHECKPOINT_DATASET_NAME}'")

QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")
logging.info(f"[Config] Check -> SERVICE_BUS_QUEUE_NAME: '{QUEUE_NAME}'")
if not QUEUE_NAME:
    logging.error("[Config] FAILURE: Missing critical environment variable: SERVICE_BUS_QUEUE_NAME")
    raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")

IDS_PER_MESSAGE_STR = os.getenv("IDS_PER_MESSAGE")
logging.info(f"[Config] Check -> IDS_PER_MESSAGE Raw String: '{IDS_PER_MESSAGE_STR}'")
if not IDS_PER_MESSAGE_STR:
    logging.error("[Config] FAILURE: Missing critical environment variable: IDS_PER_MESSAGE")
    raise ValueError("IDS_PER_MESSAGE must be set.")
try:
    IDS_PER_MESSAGE = int(IDS_PER_MESSAGE_STR)
    logging.info(f"[Config] Parsed -> IDS_PER_MESSAGE Integer Value: {IDS_PER_MESSAGE}")
except ValueError as e:
    logging.error(f"[Config] FAILURE: IDS_PER_MESSAGE is not a valid integer: {IDS_PER_MESSAGE_STR}")
    raise e

MAX_MESSAGES_PER_RUN_STR = os.getenv("MAX_MESSAGES_PER_RUN")
logging.info(f"[Config] Check -> MAX_MESSAGES_PER_RUN Raw String: '{MAX_MESSAGES_PER_RUN_STR}'")
if not MAX_MESSAGES_PER_RUN_STR:
    logging.error("[Config] FAILURE: Missing critical environment variable: MAX_MESSAGES_PER_RUN")
    raise ValueError("MAX_MESSAGES_PER_RUN must be set.")
try:
    MAX_MESSAGES_PER_RUN = int(MAX_MESSAGES_PER_RUN_STR)
    logging.info(f"[Config] Parsed -> MAX_MESSAGES_PER_RUN Integer Value: {MAX_MESSAGES_PER_RUN}")
except ValueError as e:
    logging.error(f"[Config] FAILURE: MAX_MESSAGES_PER_RUN is not a valid integer: {MAX_MESSAGES_PER_RUN_STR}")
    raise e

logging.info("[Config] Environment validation completed successfully.")


# ==========================================
# LANGFUSE CLIENT & CHECKPOINT UTILITIES
# ==========================================

def _get_langfuse_client() -> Any:
    """Retrieves and initializes the Langfuse client strictly. Raises error if components are missing."""
    logging.info("[Langfuse] Initializing Langfuse client...")
    
    if not Langfuse:
        logging.error("[Langfuse] FAILURE: Langfuse dependency is missing.")
        raise ImportError("Langfuse library is not installed.")

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    host = os.getenv("LANGFUSE_HOST")

    logging.info(f"[Langfuse] Configuration -> Public Key: {_mask_value(public_key)}, Secret Key: {_mask_value(secret_key)}, Host: '{host}'")

    if not public_key:
        logging.error("[Langfuse] FAILURE: LANGFUSE_PUBLIC_KEY environment variable is missing.")
        raise ValueError("LANGFUSE_PUBLIC_KEY is required.")
    if not secret_key:
        logging.error("[Langfuse] FAILURE: LANGFUSE_SECRET_KEY environment variable is missing.")
        raise ValueError("LANGFUSE_SECRET_KEY is required.")
    if not host:
        logging.error("[Langfuse] FAILURE: LANGFUSE_HOST environment variable is missing.")
        raise ValueError("LANGFUSE_HOST is required.")

    try:
        client = Langfuse(
            public_key=public_key.strip(),
            secret_key=secret_key.strip(),
            host=host.strip()
        )
        logging.info("[Langfuse] SUCCESS: Client successfully initialized.")
        return client
    except Exception as e:
        logging.error(f"[Langfuse] FAILURE: Client initialization failed: {e}", exc_info=True)
        raise e


def _load_checkpoint_from_langfuse() -> Optional[str]:
    """Retrieves the last processed ID from Langfuse. Returns None if no checkpoint exists."""
    logging.info(f"[Langfuse] Attempting to retrieve checkpoint from dataset '{CHECKPOINT_DATASET_NAME}'...")
    try:
        langfuse = _get_langfuse_client()
    except Exception as init_err:
        logging.warning(f"[Langfuse] Non-blocking failure to initialize Langfuse client during checkpoint load: {init_err}. Proceeding with clean start.")
        return None

    try:
        logging.info(f"[Langfuse] Requesting dataset '{CHECKPOINT_DATASET_NAME}' metadata from API...")
        dataset = langfuse.get_dataset(CHECKPOINT_DATASET_NAME)
        if not dataset:
            logging.info(f"[Langfuse] RESULT: Dataset '{CHECKPOINT_DATASET_NAME}' does not exist on Langfuse. Proceeding with clean start.")
            return None
            
        if not hasattr(dataset, 'items') or not dataset.items:
            logging.warning(f"[Langfuse] RESULT: Dataset '{CHECKPOINT_DATASET_NAME}' is empty. Proceeding with clean start.")
            return None
        
        logging.info(f"[Langfuse] Found {len(dataset.items)} items in dataset. Filtering for the latest checkpoint...")
        latest_item = max(dataset.items, key=lambda r: getattr(r, 'created_at'))
        item_id = getattr(latest_item, 'id', 'N/A')
        logging.info(f"[Langfuse] Selected newest item ID: '{item_id}' (Created At: {getattr(latest_item, 'created_at', 'N/A')})")
        
        if latest_item and hasattr(latest_item, 'input'):
            checkpoint_data = latest_item.input
            if isinstance(checkpoint_data, dict) and "last_id" in checkpoint_data:
                last_id = str(checkpoint_data["last_id"])
                logging.info(f"[Langfuse] SUCCESS: Checkpoint retrieved. Last processed ID: '{last_id}'")
                return last_id
            else:
                logging.error(f"[Langfuse] FAILURE: Checkpoint payload missing 'last_id'. Payload: {checkpoint_data}")
        else:
            logging.error(f"[Langfuse] FAILURE: Newest dataset item (ID='{item_id}') is missing the 'input' attribute.")
    except Exception as ex:
        logging.warning(f"[Langfuse] Non-blocking exception retrieving checkpoint: {ex}. Assuming clean start.", exc_info=True)
    
    return None


def _save_checkpoint_to_langfuse(last_id: str) -> None:
    """Saves the last processed record ID to Langfuse as the checkpoint."""
    logging.info(f"[Langfuse] Initiating checkpoint save for last_id: '{last_id}'...")
    try:
        langfuse = _get_langfuse_client()
    except Exception as init_err:
        logging.error(f"[Langfuse] FAILURE: Could not connect to Langfuse client to save checkpoint: {init_err}")
        raise init_err

    try:
        logging.info(f"[Langfuse] Ensuring dataset '{CHECKPOINT_DATASET_NAME}' exists or is created...")
        langfuse.create_dataset(
            name=CHECKPOINT_DATASET_NAME,
            description=f"IDP accuracy checkpoint ({LANGFUSE_ENVIRONMENT})"
        )
        
        checkpoint_item_id = f"checkpoint::id::{last_id}"
        checkpoint_payload = {
            "last_id": last_id,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        
        logging.info(f"[Langfuse] Uploading checkpoint item '{checkpoint_item_id}' to dataset '{CHECKPOINT_DATASET_NAME}'...")
        langfuse.create_dataset_item(
            dataset_name=CHECKPOINT_DATASET_NAME,
            id=checkpoint_item_id,
            input=checkpoint_payload,
            metadata={
                "record_type": "idp_checkpoint",
                "last_id": last_id,
            }
        )
        
        logging.info("[Langfuse] Flushing sync buffer...")
        langfuse.flush()
        logging.info(f"[Langfuse] SUCCESS: Checkpoint saved with last_id: '{last_id}'")
    except Exception as ex:
        logging.error(f"[Langfuse] FAILURE: Error updating checkpoint: {ex}", exc_info=True)
        raise ex


# ==========================================
# DATABASE UTILITIES
# ==========================================

def _get_db_connection() -> pymssql.Connection:
    """Builds and returns a SQL Server connection via pymssql. Raises exception on invalid config or connection failure."""
    logging.info("[Database] Initiating database connection setup...")
    server = os.getenv("IDP_SQL_SERVER")
    database = os.getenv("IDP_SQL_DATABASE")
    user = os.getenv("IDP_SQL_USER")
    password = os.getenv("IDP_SQL_PASSWORD")

    logging.info(
        f"[Database] Connection configuration check: "
        f"Server='{server}', Database='{database}', User='{user}', Password_Set={bool(password)}"
    )

    if not server:
        logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_SERVER is not set.")
        raise ValueError("IDP_SQL_SERVER is required.")
    if not database:
        logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_DATABASE is not set.")
        raise ValueError("IDP_SQL_DATABASE is required.")
    if not user:
        logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_USER is not set.")
        raise ValueError("IDP_SQL_USER is required.")
    if not password:
        logging.error("[Database] FAILURE: Missing database configuration: IDP_SQL_PASSWORD is not set.")
        raise ValueError("IDP_SQL_PASSWORD is required.")

    # Parse host and port from server string (e.g. "myserver.database.windows.net,1433")
    host = server.strip()
    port = 1433
    if "," in host:
        host, port_str = host.rsplit(",", 1)
        try:
            port = int(port_str.strip())
        except ValueError:
            logging.warning(f"[Database] Could not parse port from server string '{server}', defaulting to 1433.")

    try:
        logging.info(f"[Database] Attempting pymssql connection to Server: '{host}:{port}', Database: '{database}'...")
        connection = pymssql.connect(
            server=host,
            port=port,
            user=user,
            password=password,
            database=database,
            login_timeout=30,
            tds_version="7.4"
        )
        logging.info("[Database] SUCCESS: Database connection established successfully.")
        return connection
    except Exception as e:
        logging.error(f"[Database] FAILURE: Failed to connect to SQL Server. Server='{host}:{port}', Database='{database}', User='{user}'. Error: {e}", exc_info=True)
        raise e


# ==========================================
# QUEUE UTILITIES
# ==========================================

def _send_to_azure_queue(messages: List[str]) -> None:
    """Dispatches messages to Azure Service Bus queue."""
    logging.info(f"[Queue] Preparing to dispatch {len(messages)} messages to Azure Service Bus queue '{QUEUE_NAME}'...")
    connection_string = os.getenv("SERVICE_BUS_CONNECTION_STRING")
    if not connection_string:
        logging.error("[Queue] FAILURE: Missing configuration: SERVICE_BUS_CONNECTION_STRING is not set.")
        raise ValueError("SERVICE_BUS_CONNECTION_STRING is missing.")

    try:
        with ServiceBusClient.from_connection_string(connection_string) as client:
            with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
                logging.info(f"[Queue] Connected to Service Bus queue '{QUEUE_NAME}'.")
                for idx, msg in enumerate(messages):
                    try:
                        logging.info(f"[Queue] Dispatching message {idx + 1}/{len(messages)} (Length: {len(msg)} characters)...")
                        sender.send_messages(ServiceBusMessage(msg))
                        logging.info(f"[Queue] SUCCESS: Message {idx + 1}/{len(messages)} was sent successfully.")
                    except Exception as e:
                        logging.error(f"[Queue] FAILURE: Failed to transmit message {idx + 1}. Payload excerpt: '{msg[:200]}...'. Error details: {e}", exc_info=True)
                        raise e
    except Exception as e:
        logging.error(f"[Queue] FAILURE: Service Bus client error: {e}", exc_info=True)
        raise e
    logging.info(f"[Queue] SUCCESS: All {len(messages)} messages successfully written to Service Bus queue '{QUEUE_NAME}'.")


# ==========================================
# TIMER TASK 1 IMPLEMENTATION
# ==========================================

def idp_data_push(ids_per_message: Optional[int] = None, max_messages_per_run: Optional[int] = None) -> None:
    """Performs strict incremental retrieval and queue dispatch. Falls back to env var defaults when parameters are not supplied."""
    # Fall back to module-level env var values (used by timer trigger which passes no args)
    if ids_per_message is None:
        ids_per_message = IDS_PER_MESSAGE
    if max_messages_per_run is None:
        max_messages_per_run = MAX_MESSAGES_PER_RUN

    logging.info("=============================================================")
    logging.info("[Timer Task 1] Initiating Workflow Execution...")
    logging.info("=============================================================")
    
    run_datetime = datetime.now(timezone.utc)
    logging.info(f"[Timer Task 1] Workflow configurations -> IDs/Message: {ids_per_message}, Max Messages: {max_messages_per_run}, Exec Time: {run_datetime.isoformat()}")

    # 1. Check the checkpoint on Langfuse
    logging.info("[Timer Task 1] Step 1: Checking latest checkpoint from Langfuse...")
    last_checkpoint_id = _load_checkpoint_from_langfuse()
    
    # 2. Build the query based on checkpoint availability
    # ORDER BY v.Id ASC so pagination is consistent and deterministic
    query_params = []
    if last_checkpoint_id:
        logging.info(f"[Timer Task 1] Step 2: Checkpoint retrieved. Last processed ID: '{last_checkpoint_id}'. Executing incremental query.")
        query = (
            "SELECT DISTINCT v.Id "
            "FROM dbo.vw_PdfClassificationTransactionLog v "
            "WHERE v.Id > %s "
            "ORDER BY v.Id ASC;"
        )
        query_params.append(last_checkpoint_id)
        logging.info(f"[Timer Task 1] Query formulated with last_checkpoint_id='{last_checkpoint_id}'")
    else:
        logging.info("[Timer Task 1] Step 2: No checkpoint found. Initializing query over full dataset.")
        query = (
            "SELECT DISTINCT v.Id "
            "FROM dbo.vw_PdfClassificationTransactionLog v "
            "ORDER BY v.Id ASC;"
        )
        logging.info("[Timer Task 1] Query formulated with no parameters.")

    # 3. Fetch rows from Database
    logging.info("[Timer Task 1] Step 3: Executing prepared query on SQL Server...")
    try:
        with _get_db_connection() as conn:
            cursor = conn.cursor()
            logging.info("[Timer Task 1] Query executed. Extracting results...")
            cursor.execute(query, query_params)
            rows = cursor.fetchall()
            
            unique_keys = [str(row[0]).strip() for row in rows if row[0] is not None]
            logging.info(f"[Timer Task 1] SUCCESS: Retrieved {len(rows)} raw rows. Extracted {len(unique_keys)} clean unique IDs.")
    except Exception as e:
        logging.error(f"[Timer Task 1] FAILURE: Database step failed during query execution: {e}", exc_info=True)
        raise e

    # 4. Process and calculate keys to dispatch
    total_keys = len(unique_keys)
    logging.info(f"[Timer Task 1] Step 4: Assessing structural segments for {total_keys} unique keys.")

    if total_keys == 0:
        logging.info("[Timer Task 1] SUCCESS: No new records identified since last execution. Workflow finished cleanly.")
        return

    # Group IDs into chunks
    chunks = [unique_keys[i:i + ids_per_message] for i in range(0, total_keys, ids_per_message)]
    total_chunks = len(chunks)
    logging.info(f"[Timer Task 1] Segregation result: {total_chunks} chunk(s) calculated.")
    
    is_capped = False
    if total_chunks > max_messages_per_run:
        logging.warning(f"[Timer Task 1] CAPPING TRIGGERED: Segment chunks ({total_chunks}) exceeds system cap ({max_messages_per_run}).")
        chunks = chunks[:max_messages_per_run]
        is_capped = True
        logging.warning(f"[Timer Task 1] Workflow has been capped to the top {max_messages_per_run} chunks. Remaining items will be processed in subsequent runs.")
    else:
        logging.info(f"[Timer Task 1] Volume within execution threshold. All {total_chunks} chunk(s) are eligible to send.")

    # Format into JSON payloads
    logging.info("[Timer Task 1] Generating standard message structures...")
    formatted_messages = []
    for idx, chunk in enumerate(chunks):
        payload = {
            "record_ids": chunk,
            "source": "idp",
            "environment": LANGFUSE_ENVIRONMENT,
            "process_type": "Accuracy",
            "queued_at": datetime.now(timezone.utc).isoformat()
        }
        json_payload = json.dumps(payload)
        formatted_messages.append(json_payload)
        logging.debug(f"[Timer Task 1] Chunk {idx + 1} structured with {len(chunk)} keys.")

    # Last ID in the last dispatched chunk — used as the checkpoint bookmark
    last_dispatched_id = chunks[-1][-1]
    logging.info(f"[Timer Task 1] Last dispatched ID: '{last_dispatched_id}'")

    # 5. Push formatted payloads to the Queue
    logging.info(f"[Timer Task 1] Step 5: Commencing dispatch of {len(formatted_messages)} messages to queue '{QUEUE_NAME}'...")
    try:
        _send_to_azure_queue(formatted_messages)
        logging.info("[Timer Task 1] SUCCESS: All queue transfers completed.")
    except Exception as e:
        logging.error(f"[Timer Task 1] FAILURE: Queue dispatch failed: {e}", exc_info=True)
        raise e

    # 6. Save checkpoint to Langfuse using the last dispatched ID
    # - Capped run: last ID in the last dispatched chunk (partial progress bookmark)
    # - Full run: same — last ID of all dispatched records
    if is_capped:
        logging.info(f"[Timer Task 1] Step 6: Capped run — saving partial progress checkpoint. Last dispatched ID: '{last_dispatched_id}'")
    else:
        logging.info(f"[Timer Task 1] Step 6: Full run — saving checkpoint. Last dispatched ID: '{last_dispatched_id}'")

    try:
        _save_checkpoint_to_langfuse(last_dispatched_id)
        logging.info(f"[Timer Task 1] SUCCESS: Checkpoint saved to Langfuse with last_id: '{last_dispatched_id}'")
    except Exception as e:
        logging.error(f"[Timer Task 1] FAILURE: Langfuse checkpoint sync failed: {e}", exc_info=True)
        raise e

    logging.info("=============================================================")
    logging.info("[Timer Task 1] Workflow execution successfully completed.")
    logging.info("=============================================================")


if __name__ == "__main__":
    logging.info("Initiating manual trigger execution...")
    try:
        idp_data_push(
            ids_per_message=IDS_PER_MESSAGE,
            max_messages_per_run=MAX_MESSAGES_PER_RUN
        )
    except Exception as main_err:
        logging.critical(f"Workflow execution halted by uncaught critical exception: {main_err}", exc_info=True)
        raise main_err