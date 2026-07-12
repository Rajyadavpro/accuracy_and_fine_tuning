import requests
import json

# Change this to your deployed Azure Function URL when testing in production
# e.g., "https://<your-app-name>.azurewebsites.net/api/run_http_tasks"
BASE_URL = "http://localhost:7071/api/run_http_tasks"

def trigger_via_get(task_id, ids_per_message=None, max_messages=None):
    """
    Sends a GET request to trigger the task using URL query parameters.
    """
    params = {
        "task_id": task_id
    }
    if ids_per_message is not None:
        params["ids_per_message"] = ids_per_message
    if max_messages is not None:
        params["max_messages_per_run"] = max_messages

    print(f"\n--- Sending GET request to {BASE_URL} ---")
    print(f"Parameters: {params}")
    
    try:
        response = requests.get(BASE_URL, params=params)
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the Azure Function. Is your local host running?")

def trigger_via_post(task_id, ids_per_message=None, max_messages=None):
    """
    Sends a POST request with the execution options passed in the JSON body.
    """
    payload = {
        "task_id": str(task_id)
    }
    if ids_per_message is not None:
        payload["ids_per_message"] = ids_per_message
    if max_messages is not None:
        payload["max_messages_per_run"] = max_messages

    headers = {
        "Content-Type": "application/json"
    }

    print(f"\n--- Sending POST request to {BASE_URL} ---")
    print(f"JSON Payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(BASE_URL, json=payload, headers=headers)
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the Azure Function. Is your local host running?")

if __name__ == "__main__":
    # --- TEST CASES ---
    
    # Test Case 1: Trigger IDP task (task 1) via GET
    # Batches 5 IDs per message, with a maximum of 3 messages total
    # trigger_via_get(task_id="1", ids_per_message=1, max_messages=1)

    # Test Case 2: Trigger Tabak task (task 2) via POST
    # Batches 10 IDs per message, with a maximum of 2 messages total
    # trigger_via_post(task_id="2", ids_per_message=1, max_messages=1)

    # Test Case 3: Trigger Healthcare task (task 3) via GET (Minimal run)
    # Batches 1 ID per message, with a maximum of 1 message total
    # trigger_via_get(task_id="3", ids_per_message=1, max_messages=1)

    # Test Case 4: Trigger Tabak Fine Tuning task (task 4) via POST
    # trigger_via_post(task_id="4", ids_per_message=1, max_messages=1)
    
    # Test Case 5: Trigger EOB Fine Tuning task (task 5) via POST
    trigger_via_post(task_id="5", ids_per_message=1, max_messages=1)