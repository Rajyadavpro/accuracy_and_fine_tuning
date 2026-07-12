#!/usr/bin/env python
import requests
import time

BASE_URL = "http://localhost:7071/api/run_http_tasks"

print("=" * 60)
print("TRIGGERING ALL THREE DATA PIPELINES")
print("=" * 60)

for task_id in [1, 2, 3]:
    task_names = {1: "IDP", 2: "Tabak", 3: "Healthcare"}
    print(f"\n>>> Triggering Task {task_id}: {task_names[task_id]}")
    
    try:
        response = requests.get(f"{BASE_URL}?task_id={task_id}", timeout=30)
        print(f"    Status Code: {response.status_code}")
        print(f"    Response: {response.text}")
    except requests.exceptions.Timeout:
        print(f"    ERROR: Request timed out (task may still be running)")
    except requests.exceptions.ConnectionError as e:
        print(f"    ERROR: Connection failed - {e}")
    except Exception as e:
        print(f"    ERROR: {e}")
    
    time.sleep(3)

print("\n" + "=" * 60)
print("ALL TASKS TRIGGERED - Check logs for results")
print("=" * 60)
