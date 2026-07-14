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
DEFAULT_MAX_MESSAGES = 1  # Use None to process all available records

def trigger_task(task_id: str, task_name: str, ids_per_msg: int = DEFAULT_IDS_PER_MESSAGE, max_msgs: int = DEFAULT_MAX_MESSAGES):
    """
    Helper function to trigger a specific task via the HTTP endpoint.
    """
    print(f"\n[{task_name}] Triggering Task ID {task_id}...")
    
    payload = {
        "task_id": task_id,
        "ids_per_message": ids_per_msg
    }
    
    if max_msgs is not None:
        payload["max_messages_per_run"] = max_msgs

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
if __name__ == "__main__":
    print("Starting sequential task execution...")

    # ACCURACY SCRIPTS
    # trigger_task("1", "IDP Accuracy")
    # trigger_task("2", "Tabak Accuracy")
    # trigger_task("3", "Healthcare EOB Accuracy")
    # trigger_task("4", "Healthcare Superbill Accuracy")

    # # FINE TUNING DATA PUSH SCRIPTS
    # trigger_task("5", "Tabak Fine Tuning")
    # trigger_task("6", "EOB Fine Tuning")
    # trigger_task("7", "Superbill Fine Tuning")
    trigger_task("8", "IDP Fine Tuning Data Push")


    print("\nAll selected tasks have finished executing.")