import json
from pathlib import Path

import requests
from langfuse import Langfuse


DATASETS = [
    "healthcare_accuracy_eob_exp2",
    "healthcare_accuracy_superbill_exp2",
    "healthcare_predictions_eob_exp2",
    "healthcare_predictions_superbill_exp2",
]


def get_client() -> Langfuse:
    cfg = json.loads(Path("local.settings.json").read_text(encoding="utf-8"))["Values"]
    return Langfuse(
        public_key=cfg["LANGFUSE_PUBLIC_KEY"],
        secret_key=cfg["LANGFUSE_SECRET_KEY"],
        host=cfg["LANGFUSE_HOST"],
    )


def clear_dataset_items(client: Langfuse, dataset_name: str) -> None:
    try:
        page = 1
        ids = []
        while True:
            resp = client.api.dataset_items.list(dataset_name=dataset_name, page=page, limit=100)
            if not resp or not resp.data:
                break
            ids.extend([item.id for item in resp.data])
            page += 1

        for item_id in ids:
            client.api.dataset_items.delete(id=item_id)

        print(f"cleared dataset={dataset_name}, deleted_items={len(ids)}")
    except Exception as exc:
        print(f"skip clear dataset={dataset_name}, reason={exc}")


def show_dataset(client: Langfuse, dataset_name: str) -> None:
    try:
        ds = client.get_dataset(dataset_name)
        items = getattr(ds, "items", []) or []
        print(f"dataset={dataset_name}, items_count={len(items)}")
        if items:
            latest = max(items, key=lambda i: getattr(i, "created_at"))
            print(f"latest_item_id={getattr(latest, 'id', '')}")
            print(json.dumps(getattr(latest, "input", None), ensure_ascii=True, indent=2, default=str))
    except Exception as exc:
        print(f"dataset={dataset_name}, status=NOT_FOUND_OR_ERROR, detail={exc}")


def trigger_healthcare() -> None:
    url = "http://localhost:7071/api/run_http_tasks"
    params = {"task_id": "3", "ids_per_message": 1, "max_messages_per_run": 1}
    r = requests.get(url, params=params, timeout=180)
    print(f"trigger_status={r.status_code}")
    print(f"trigger_body={r.text}")


def main() -> None:
    client = get_client()

    print("=== Clearing datasets ===")
    for ds in DATASETS:
        clear_dataset_items(client, ds)

    print("=== Trigger healthcare task ===")
    trigger_healthcare()

    print("=== Showing datasets ===")
    for ds in DATASETS:
        show_dataset(client, ds)


if __name__ == "__main__":
    main()
