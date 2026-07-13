import json
from pathlib import Path

from langfuse import Langfuse


def main() -> None:
    cfg = json.loads(Path("local.settings.json").read_text(encoding="utf-8"))["Values"]
    client = Langfuse(
        public_key=cfg["LANGFUSE_PUBLIC_KEY"],
        secret_key=cfg["LANGFUSE_SECRET_KEY"],
        host=cfg["LANGFUSE_HOST"],
    )

    dataset_names = [
        "idp_accuracy_checkpoint_exp",
        "idp_predictions_exp",
        "idp_accuracy_checkpoint_exp2",
        "idp_predictions_exp2",
    ]

    for name in dataset_names:
        try:
            ds = client.get_dataset(name)
            items = len(getattr(ds, "items", []) or []) if ds else 0
            print(f"{name}: EXISTS={bool(ds)}, ITEMS={items}")
        except Exception as exc:
            print(f"{name}: ERROR={exc}")


if __name__ == "__main__":
    main()
