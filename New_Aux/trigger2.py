import requests
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Change this to your deployed Azure Function URL when testing in production
# e.g., "https://<your-app-name>.azurewebsites.net/api/run_http_tasks"
BASE_URL = "http://localhost:7071/api/run_http_tasks"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 1800

def trigger_via_get(task_id, ids_per_message=None, max_messages=None, file_type=None, timeout_seconds=DEFAULT_REQUEST_TIMEOUT_SECONDS):
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
    if file_type:
        params["file_type"] = file_type

    print(f"\n--- Sending GET request to {BASE_URL} ---")
    print(f"Parameters: {params}")
    print(f"Timeout (seconds): {timeout_seconds}")
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=timeout_seconds)
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the Azure Function. Is your local host running?")
    except requests.exceptions.Timeout:
        print(f"Error: Request timed out after {timeout_seconds} seconds.")

def trigger_via_post(task_id, ids_per_message=None, max_messages=None, file_type=None, timeout_seconds=DEFAULT_REQUEST_TIMEOUT_SECONDS):
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
    if file_type:
        payload["file_type"] = file_type

    headers = {
        "Content-Type": "application/json"
    }

    print(f"\n--- Sending POST request to {BASE_URL} ---")
    print(f"JSON Payload: {json.dumps(payload, indent=2)}")
    print(f"Timeout (seconds): {timeout_seconds}")
    
    try:
        response = requests.post(BASE_URL, json=payload, headers=headers, timeout=timeout_seconds)
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the Azure Function. Is your local host running?")
    except requests.exceptions.Timeout:
        print(f"Error: Request timed out after {timeout_seconds} seconds.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trigger Azure Function HTTP tasks.")
    parser.add_argument(
        "--task-id",
        nargs="+",
        help="Task ID(s) to run. Supports '--task-id 1 2 3' or '--task-id 1,2,3'.",
    )
    parser.add_argument("--method", choices=["get", "post", "auto"], default="auto", help="HTTP method to use.")
    parser.add_argument("--ids-per-message", type=int, help="Optional ids_per_message override.")
    parser.add_argument("--max-messages", type=int, help="Optional max_messages_per_run override.")
    parser.add_argument("--file-type", choices=["both", "eob", "superbill"], help="Healthcare file type override for task 3.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_REQUEST_TIMEOUT_SECONDS, help="Request timeout in seconds.")
    parser.add_argument("--parallel", action="store_true", help="Run selected task IDs in parallel.")
    parser.add_argument("--max-workers", type=int, default=3, help="Worker count for --parallel mode.")
    parser.add_argument("--run-all", action="store_true", help="Run the legacy sequence of task 1, 2, and 3.")
    return parser.parse_args()


def _parse_task_ids(raw_values: list[str] | None) -> list[str]:
    if not raw_values:
        return []
    allowed = {"1", "2", "3", "4", "5", "6", "7"}
    parsed: list[str] = []
    for raw in raw_values:
        for part in raw.split(","):
            task_id = part.strip()
            if not task_id:
                continue
            if task_id not in allowed:
                raise ValueError(
                    f"Invalid task id '{task_id}'. Allowed values: {', '.join(sorted(allowed))}."
                )
            parsed.append(task_id)
    return parsed


def _run_one(task_id: str, method: str, ids_per_message: int | None, max_messages: int | None, file_type: str | None, timeout_seconds: int) -> None:
    # Keep backward-compatible default methods for known tasks.
    resolved_method = method
    if resolved_method == "auto":
        resolved_method = "post" if task_id in {"2", "4", "5"} else "get"

    if resolved_method == "post":
        trigger_via_post(
            task_id=task_id,
            ids_per_message=ids_per_message,
            max_messages=max_messages,
            file_type=file_type,
            timeout_seconds=timeout_seconds,
        )
    else:
        trigger_via_get(
            task_id=task_id,
            ids_per_message=ids_per_message,
            max_messages=max_messages,
            file_type=file_type,
            timeout_seconds=timeout_seconds,
        )


def _run_many(task_ids: list[str], method: str, ids_per_message: int | None, max_messages: int | None, file_type: str | None, timeout_seconds: int, parallel: bool, max_workers: int) -> None:
    if not parallel or len(task_ids) <= 1:
        for task_id in task_ids:
            _run_one(task_id, method, ids_per_message, max_messages, file_type, timeout_seconds)
        return

    workers = max(1, min(max_workers, len(task_ids)))
    print(f"Running tasks in parallel: {task_ids} (workers={workers})")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_run_one, task_id, method, ids_per_message, max_messages, file_type, timeout_seconds): task_id
            for task_id in task_ids
        }
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                future.result()
                print(f"Task {task_id} request completed.")
            except Exception as ex:
                print(f"Task {task_id} request failed: {ex}")

if __name__ == "__main__":
    args = parse_args()
    try:
        task_ids = _parse_task_ids(args.task_id)
    except ValueError as ex:
        print(str(ex))
        raise SystemExit(2)

    if args.run_all:
        _run_many(
            task_ids=["1", "2", "3"],
            method=args.method,
            ids_per_message=args.ids_per_message,
            max_messages=args.max_messages,
            file_type=args.file_type,
            timeout_seconds=args.timeout,
            parallel=args.parallel,
            max_workers=args.max_workers,
        )
    elif task_ids:
        _run_many(
            task_ids=task_ids,
            method=args.method,
            ids_per_message=args.ids_per_message,
            max_messages=args.max_messages,
            file_type=args.file_type,
            timeout_seconds=args.timeout,
            parallel=args.parallel,
            max_workers=args.max_workers,
        )
    else:
        print("No task selected. Use --task-id <1-7> (single/multiple) or --run-all.")
