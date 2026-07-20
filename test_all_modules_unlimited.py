#!/usr/bin/env python
"""Test all 5 modules with max_msgs=-1 (unlimited)"""
import requests
import time

BASE_URL = "http://localhost:7071/api/run_http_tasks"

def test_task(task_id, task_name):
    """Trigger a single task with max_msgs=-1"""
    print(f"\n{'='*60}")
    print(f"Testing Task {task_id}: {task_name}")
    print(f"{'='*60}")
    
    payload = {
        "task_id": task_id,
        "ids_per_message": 10,
        "max_messages_per_run": -1,  # Unlimited
        "folder_name": "test_unlimited"
    }
    
    try:
        response = requests.get(BASE_URL, json=payload, timeout=120)
        if response.status_code == 200:
            print(f"✓ Task {task_id} SUCCESS: {response.text}")
        else:
            print(f"✗ Task {task_id} FAILED (Status {response.status_code}): {response.text}")
    except Exception as e:
        print(f"✗ Task {task_id} ERROR: {e}")
    
    time.sleep(2)

if __name__ == "__main__":
    print("Testing all 5 modules with max_msgs=-1 (unlimited)")
    
    # Test each fine-tuning module with unlimited messages
    test_task("5", "Tabak Fine Tuning")
    test_task("6", "EOB Fine Tuning")
    test_task("7", "Superbill Fine Tuning")
    test_task("8", "IDP Fine Tuning")
    test_task("9", "CCAI Fine Tuning")
    
    print(f"\n{'='*60}")
    print("All tasks triggered. Waiting 30 seconds before checking queue...")
    time.sleep(30)
    
    print("Checking Service Bus queue...")
    import subprocess
    result = subprocess.run(
        ["python", "AUX_code/fetch_servicebus_messages.py"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
