#!/usr/bin/env python3
"""
Unified Orchestrator:
1. Truncates all tables in ClickHouse.
2. Purges the designated Azure Service Bus queue.
3. Recursively deletes the fetched_messages target directory.
All configurations are dynamically loaded from local.settings.json.
"""

import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
import requests

try:
    from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
except ImportError:
    print("[-] Error: 'azure-servicebus' package is not installed.")
    print("    Please install it using: pip install azure-servicebus")
    sys.exit(1)

# --- PATH & LOGGER CONFIGURATION ---
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_FILE = SCRIPT_DIR / "data_push.log"

# Default folder to delete if none is specified as a command-line argument
DEFAULT_FOLDER_TO_DELETE = r"C:\Users\raj.kumaryadav\Desktop\Superbill\Main_Git_repo\Accuracy_and_Fine_tuning_f1\fetched_messages"

# Setup Unified Logging (to both file and terminal)
logger = logging.getLogger("UNIFIED_CLEANUP_PIPELINE")
logger.setLevel(logging.DEBUG)

# File handler
fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] (%(name)s:%(lineno)d) - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(fh)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger.addHandler(ch)


def load_settings() -> dict:
    """Attempts to find and load local.settings.json from common relative paths."""
    possible_paths = [
        Path.cwd() / "local.settings.json",
        SCRIPT_DIR / "local.settings.json",
        SCRIPT_DIR.parent / "local.settings.json"
    ]
    
    for path in possible_paths:
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    settings = json.load(f).get("Values", {})
                    logger.debug(f"[Config] Successfully loaded config from '{path}'")
                    return settings
            except Exception as e:
                logger.warning(f"[Config] Found settings file at '{path}' but failed to parse: {e}")
                
    logger.warning("[Config] local.settings.json not found in common paths. Falling back to default values.")
    return {}


# =====================================================================
# CLICKHOUSE CLEANUP LOGIC
# =====================================================================

def get_clickhouse_config(settings: dict):
    """Retrieve ClickHouse connection parameters from settings or default values."""
    host = settings.get("CLICKHOUSE_HOST", "172.173.148.33")
    http_port = int(settings.get("CLICKHOUSE_HTTP_PORT", "8123"))
    database = settings.get("CLICKHOUSE_DATABASE", "accuracy_and_finetuning")
    user = settings.get("CLICKHOUSE_USER", "admin")
    password = settings.get("CLICKHOUSE_PASSWORD", "Holly7583hfxZ")
    return host, http_port, database, user, password


def clickhouse_request(host, http_port, user, password, query: str, is_post: bool = False, timeout: int = 10) -> requests.Response:
    """Helper to dispatch GET or POST requests to the ClickHouse HTTP interface."""
    url = f"http://{host}:{http_port}/"
    if is_post:
        return requests.post(url, auth=(user, password), timeout=timeout, data=query)
    else:
        return requests.get(url, auth=(user, password), timeout=timeout, params={"query": query})


def get_all_tables(host, http_port, database, user, password):
    """Retrieves a list of all tables inside the targeted ClickHouse database."""
    try:
        query = f"SHOW TABLES FROM `{database}`"
        logger.debug(f"[ClickHouse] Fetching tables list from database '{database}'...")
        response = clickhouse_request(host, http_port, user, password, query)
        
        if response.status_code == 200:
            return [t.strip() for t in response.text.strip().split('\n') if t.strip()]
        else:
            logger.error(f"[ClickHouse] Failed to fetch tables: {response.status_code} - {response.text[:200]}")
            return None
    except Exception as ex:
        logger.error(f"[ClickHouse] Exception fetching table list: {ex}")
        return None


def get_record_count(host, http_port, database, user, password, table_name):
    """Gets the current row count of a targeted table."""
    try:
        query = f"SELECT COUNT(*) as count FROM `{database}`.`{table_name}`"
        response = clickhouse_request(host, http_port, user, password, query)
        
        if response.status_code == 200:
            return int(response.text.strip())
        else:
            logger.error(f"[ClickHouse] Failed to get count for {table_name}: {response.status_code}")
            return None
    except Exception as ex:
        logger.error(f"[ClickHouse] Exception getting count for {table_name}: {ex}")
        return None


def truncate_table(host, http_port, database, user, password, table_name) -> bool:
    """Truncates (deletes all data from) a specific table."""
    try:
        truncate_sql = f"TRUNCATE TABLE `{database}`.`{table_name}`"
        logger.info(f"[ClickHouse] Sending TRUNCATE query to '{table_name}'...")
        response = clickhouse_request(host, http_port, user, password, truncate_sql, is_post=True, timeout=30)
        
        if response.status_code in [200, 202]:
            logger.info(f"[ClickHouse] Table '{table_name}' truncated successfully!")
            return True
        else:
            logger.error(f"[ClickHouse] Truncate failed on '{table_name}': {response.status_code} - {response.text[:500]}")
            return False
    except Exception as ex:
        logger.error(f"[ClickHouse] Exception truncating table '{table_name}': {ex}")
        return False


def run_clickhouse_cleanup(settings: dict) -> bool:
    """Coordinates the retrieval, display, deletion, and verification of ClickHouse tables."""
    print("\n" + "="*80)
    print(" 1. CLICKHOUSE DATABASE TRUNCATION ".center(80, "="))
    print("="*80)

    host, http_port, database, user, password = get_clickhouse_config(settings)
    logger.info(f"[ClickHouse] Target Database: '{database}' on {host}")

    tables = get_all_tables(host, http_port, database, user, password)
    if tables is None:
        logger.error("[ClickHouse] Aborting DB cleanup. Table retrieval failed.")
        return False
    if not tables:
        logger.info(f"[ClickHouse] Database '{database}' has no tables. Skipping.")
        return True

    # Gather baseline statistics
    table_stats = {}
    for table in tables:
        count = get_record_count(host, http_port, database, user, password, table)
        table_stats[table] = count if count is not None else "Unknown"

    print(f"\n📊 Current ClickHouse Database Contents:")
    for table, count in table_stats.items():
        print(f"  • {table}: {count} records")

    total_records = sum([c for c in table_stats.values() if isinstance(c, int)])
    if total_records == 0:
        logger.info("[ClickHouse] All tables are already empty. No records to truncate.")
        return True

    # Execution phase
    successful_truncates = 0
    for table in tables:
        if truncate_table(host, http_port, database, user, password, table):
            successful_truncates += 1

    # Verification phase
    print("\n[ClickHouse] Verifying results...")
    time.sleep(1)  # Brief pause to ensure metadata syncs
    
    success_status = True
    for table in tables:
        final_count = get_record_count(host, http_port, database, user, password, table)
        if final_count == 0:
            print(f"  ✅ {table}: cleared successfully (0 records)")
        else:
            print(f"  ❌ {table}: failed to clear ({final_count} records remaining)")
            success_status = False

    return success_status and (successful_truncates == len(tables))


# =====================================================================
# SERVICE BUS PURGE LOGIC
# =====================================================================

def run_servicebus_purge(settings: dict) -> bool:
    """Purges all messages from the Service Bus queue in RECEIVE_AND_DELETE mode."""
    print("\n" + "="*80)
    print(" 2. SERVICE BUS QUEUE PURGING ".center(80, "="))
    print("="*80)

    conn_str = settings.get("SERVICE_BUS_CONNECTION_STRING")
    queue_name = settings.get("SERVICE_BUS_QUEUE_NAME")

    if not conn_str or "your-namespace" in conn_str or not queue_name:
        logger.error("[Service Bus] Valid Connection String or Queue Name not found in configurations.")
        return False

    try:
        logger.info(f"[Service Bus] Starting to clear messages from '{queue_name}'...")
        total_purged = 0
        
        # Connect in RECEIVE_AND_DELETE mode to automatically remove messages as soon as they are pulled
        with ServiceBusClient.from_connection_string(conn_str) as client:
            with client.get_queue_receiver(queue_name, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as receiver:
                while True:
                    # Request batches up to 1000 messages with a 3-second empty-queue timeout
                    messages = receiver.receive_messages(max_message_count=1000, max_wait_time=3)
                    if not messages:
                        break
                    
                    total_purged += len(messages)
                    print(f"[*] Cleared {len(messages)} messages (Running Total: {total_purged})...")
                    
        logger.info(f"[Service Bus] Completed. Purged {total_purged} messages from '{queue_name}'.")
        return True
    except Exception as e:
        logger.error(f"[Service Bus] Error purging queue: {e}")
        return False


# =====================================================================
# DIRECTORY DELETION LOGIC
# =====================================================================

def delete_directory(folder_path: str) -> bool:
    """Recursively deletes the specified target directory and all its files."""
    print("\n" + "="*80)
    print(" 3. LOCAL DIRECTORY DELETION ".center(80, "="))
    print("="*80)

    path = Path(folder_path)
    logger.info(f"[Directory] Deletion target: {path.resolve()}")

    if not path.exists():
        logger.info(f"[Directory] Target path '{folder_path}' does not exist. Skipping.")
        return True
    
    if not path.is_dir():
        logger.error(f"[Directory] Error: Path '{folder_path}' is a file, not a directory.")
        return False

    try:
        shutil.rmtree(path)
        logger.info(f"[Directory] Directory and all contents successfully deleted.")
        return True
    except PermissionError:
        logger.error(f"[Directory] Error: Permission denied. Some files in '{folder_path}' are locked.")
        return False
    except Exception as e:
        logger.error(f"[Directory] Error deleting folder '{folder_path}': {e}")
        return False


# =====================================================================
# MAIN PIPELINE ORCHESTRATOR
# =====================================================================

def main():
    print("\n" + "╔" + "="*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "UNIFIED SYSTEM CLEANUP ORCHESTRATOR".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "="*78 + "╝\n")

    logger.info("[START] Unified cleanup job initiated.")

    # 1. Choose local directory target (CLI override or script default)
    if len(sys.argv) > 1:
        target_folder = sys.argv[1]
    else:
        target_folder = DEFAULT_FOLDER_TO_DELETE

    # 2. Extract configuration settings
    settings = load_settings()

    # 3. Step 1: Run Clickhouse Truncations
    clickhouse_success = run_clickhouse_cleanup(settings)

    # 4. Step 2: Run Service Bus Purging
    servicebus_success = run_servicebus_purge(settings)

    # 5. Step 3: Run Directory Deletion
    folder_deleted = delete_directory(target_folder)

    # 6. Summarize Execution Status
    print("\n" + "="*60)
    print(" CLEANUP JOB SUMMARY ".center(60, "="))
    print("="*60)
    print(f"  • ClickHouse Cleanup:  {'SUCCESS' if clickhouse_success else 'FAILED'}")
    print(f"  • Service Bus Purge:   {'SUCCESS' if servicebus_success else 'FAILED'}")
    print(f"  • Directory Deletion:  {'SUCCESS' if folder_deleted else 'FAILED or SKIPPED'}")
    print("="*60 + "\n")

    logger.info("[COMPLETE] Cleanup job terminated.")

    # Return error code to terminal if critical tasks failed
    if not (clickhouse_success and servicebus_success):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Operation cancelled by user.")
        logger.warning("[Interrupted] Operation cancelled by user.")
        sys.exit(1)
    except Exception as err:
        print(f"\n\n❌ Fatal Orchestrator Error: {err}")
        logger.critical(f"[Fatal] {type(err).__name__}: {err}", exc_info=True)
        sys.exit(1)