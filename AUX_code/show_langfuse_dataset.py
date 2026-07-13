import json
import sys
from pathlib import Path

from langfuse import Langfuse


def main() -> None:
    cfg = json.loads(Path("local.settings.json").read_text(encoding="utf-8"))["Values"]
    env = (cfg.get("LANGFUSE_ENVIRONMENT") or "").strip()

    if len(sys.argv) > 1:
        dataset_name = sys.argv[1].strip()
    else:
        dataset_name = f"healthcare_accuracy_eob_{env}"

    client = Langfuse(
        public_key=cfg["LANGFUSE_PUBLIC_KEY"],
        secret_key=cfg["LANGFUSE_SECRET_KEY"],
        host=cfg["LANGFUSE_HOST"],
    )

    ds = client.get_dataset(dataset_name)
    items = getattr(ds, "items", []) or []

    print(f"dataset={dataset_name}")
    print(f"items_count={len(items)}")

    if not items:
        return

    sorted_items = sorted(items, key=lambda i: getattr(i, "created_at"), reverse=True)
    for idx, item in enumerate(sorted_items[:5], 1):
        print("-" * 80)
        print(f"item_{idx}_id={getattr(item, 'id', '')}")
        print(f"created_at={getattr(item, 'created_at', '')}")
        input_payload = getattr(item, "input", None)
        print("input=")
        print(json.dumps(input_payload, ensure_ascii=True, default=str, indent=2))


if __name__ == "__main__":
    main()
