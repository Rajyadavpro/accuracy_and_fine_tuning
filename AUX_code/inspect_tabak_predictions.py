import json
from pathlib import Path

from langfuse import Langfuse


def main() -> None:
    cfg = json.loads(Path("local.settings.json").read_text(encoding="utf-8"))["Values"]
    env = cfg.get("LANGFUSE_ENVIRONMENT", "").strip()
    ds_name = f"tabak_predictions_{env}"

    client = Langfuse(
        public_key=cfg["LANGFUSE_PUBLIC_KEY"],
        secret_key=cfg["LANGFUSE_SECRET_KEY"],
        host=cfg["LANGFUSE_HOST"],
    )

    ds = client.get_dataset(ds_name)
    items = getattr(ds, "items", []) or []
    print(f"dataset={ds_name}")
    print(f"items={len(items)}")

    if not items:
        return

    latest = max(items, key=lambda r: getattr(r, "created_at"))
    payload = latest.input if isinstance(latest.input, dict) else {"value": latest.input}
    print(f"latest_item_id={getattr(latest, 'id', '')}")
    print("latest_payload=")
    print(json.dumps(payload, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
