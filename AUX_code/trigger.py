import requests
import time

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
# Replace with your deployed Azure Function URL if running in the cloud.
# E.g., "https://<your-app-name>.azurewebsites.net/api/run_http_tasks"
BASE_URL = "http://localhost:7071/api/run_http_tasks"

# Default parameters to send to the functions
DEFAULT_IDS_PER_MESSAGE = 10
DEFAULT_MAX_MESSAGES = None  # Use None to process all available records

def trigger_task(
    task_id: str,
    task_name: str,
    ids_per_msg: int = DEFAULT_IDS_PER_MESSAGE,
    max_msgs: int = DEFAULT_MAX_MESSAGES,
    start_date: str = None,
    end_date: str = None,
    folder_name: str = None,
    bypass_checkpoint: bool = False,
):
    """
    Helper function to trigger a specific task via the HTTP endpoint.
    
    Args:
        task_id: Task identifier (1-9)
        task_name: Human-readable task name for logging
        ids_per_msg: Number of IDs per Service Bus message
        max_msgs: Maximum messages to dispatch per run (None = unlimited)
        start_date: Start date filter 'YYYY-MM-DD HH:MM:SS' (fine-tuning tasks only)
        end_date: End date filter 'YYYY-MM-DD HH:MM:SS' (fine-tuning tasks only, defaults to now)
        folder_name: Logical folder/group name for checkpoint isolation (default: 'main')
        bypass_checkpoint: If True, skip checkpoint and use start_date/end_date directly
    """
    print(f"\n[{task_name}] Triggering Task ID {task_id}...")
    
    payload = {
        "task_id": task_id,
        "ids_per_message": ids_per_msg
    }
    
    # Send max_msgs to payload (handle -1 in function as unlimited)
    if max_msgs is not None:
        payload["max_messages_per_run"] = max_msgs
    if start_date is not None:
        payload["start_date"] = start_date
    if end_date is not None:
        payload["end_date"] = end_date
    if folder_name is not None:
        payload["folder_name"] = folder_name
    if bypass_checkpoint:
        payload["bypass_checkpoint"] = "true"

    try:
        response = requests.get(BASE_URL, json=payload)
        if response.status_code == 200:
            print(f"[{task_name}] Success: {response.text}")
        else:
            print(f"[{task_name}] Failed (Status {response.status_code}): {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"[{task_name}] Request Error: {e}")
        
    # Optional: Short delay between requests to avoid overloading local resources
    time.sleep(2)

# ---------------------------------------------------------
# TASK EXECUTION LIST
# ---------------------------------------------------------
# Comment out any task below (using #) that you DO NOT want to run.
# You can also override the default parameters for specific tasks.
#
# Fine-tuning parameter examples:
#   start_date="2024-01-01 00:00:00"   -> process records from this date
#   end_date="2024-12-31 23:59:59"     -> process records up to this date
#   folder_name="main"                 -> checkpoint/output group (default: 'main')
#   bypass_checkpoint=True             -> ignore checkpoint, use start_date/end_date directly
#
# Container names are always read from env vars (credentials) — not passed here.
# If start_date is omitted, the oldest date is fetched from the source automatically.
# If end_date is omitted, the current datetime is used.
# If folder_name is omitted, 'main' is used and checkpoint table is e.g. tabak_finetuning_checkpoint_main.
if __name__ == "__main__":
    print("Starting sequential task execution...")

    # # ACCURACY SCRIPTS
    # trigger_task("1", "IDP Accuracy")
    # trigger_task("2", "Tabak Accuracy")
    # trigger_task("3", "Healthcare EOB Accuracy")
    # trigger_task("4", "Healthcare Superbill Accuracy")

    # FINE TUNING DATA PUSH SCRIPTS
    # Using ids_per_msg=2 for fine tuning because each record contains large JSON payload
    trigger_task("5", "Tabak Fine Tuning", ids_per_msg=10, max_msgs=1, folder_name="main2")
    trigger_task("6", "EOB Fine Tuning", ids_per_msg=10, max_msgs=1, folder_name="main2")
    trigger_task("7", "Superbill Fine Tuning", ids_per_msg=10, max_msgs=1, folder_name="main2")
    # # trigger_task("8", "IDP Fine Tuning Data Push", ids_per_msg=2, max_msgs=1, folder_name="main2")
    trigger_task("9", "Audio (CCAI) Fine Tuning", ids_per_msg=10, max_msgs=1, folder_name="demo2" 
                 , bypass_checkpoint=True)

    # FINE TUNING WITH DATE RANGE (bypass checkpoint example)
    # trigger_task("5", "Tabak Fine Tuning", ids_per_msg=100,max_msgs=1,
    #              folder_name="backfill-2024",
    #              start_date="2024-01-01 00:00:00", end_date="2026-12-31 23:59:59",
    #              bypass_checkpoint=False)


    # trigger_task("6", "EOB Fine Tuning", ids_per_msg=2, max_msgs=1,
    #              folder_name="backfill-2024",
    #              start_date="2024-06-01 00:00:00", bypass_checkpoint=True)


    # trigger_task("8", "IDP Fine Tuning Data Push", ids_per_msg=2, max_msgs=1,
    #              folder_name="backfill-2024", bypass_checkpoint=True)



    print("\nAll selected tasks have finished executing.")
