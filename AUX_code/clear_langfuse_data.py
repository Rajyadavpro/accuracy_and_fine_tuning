import os
import json
import logging
from pathlib import Path
from langfuse import Langfuse

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")

# Define the datasets you wish to clean up
DATASETS_TO_CLEAN = [
    "healthcare_accuracy_eob_exp",
    "healthcare_accuracy_superbill_exp",
    "tabak_finetuning_checkpoint_exp",
    "tabak_accuracy_checkpoint_exp",
    "idp_accuracy_checkpoint_exp"
]


def _mask_secret(value: str) -> str:
    if not value:
        return "<missing>"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _load_local_settings_if_available() -> None:
    """Load env vars from local.settings.json when running script directly."""
    settings_path = Path(__file__).resolve().parents[1] / "local.settings.json"
    if not settings_path.exists():
        return

    try:
        with settings_path.open("r", encoding="utf-8") as f:
            values = json.load(f).get("Values", {})
        for key, value in values.items():
            if key not in os.environ and value is not None:
                os.environ[key] = str(value)
    except Exception as e:
        logging.warning(f"Could not load local settings from '{settings_path}': {e}")


def _get_langfuse_client() -> Langfuse:
    _load_local_settings_if_available()

    public_key = (os.getenv("LANGFUSE_PUBLIC_KEY") or "").strip()
    secret_key = (os.getenv("LANGFUSE_SECRET_KEY") or "").strip()
    host = (os.getenv("LANGFUSE_HOST") or "").strip()

    if not public_key or not secret_key or not host:
        missing = []
        if not public_key:
            missing.append("LANGFUSE_PUBLIC_KEY")
        if not secret_key:
            missing.append("LANGFUSE_SECRET_KEY")
        if not host:
            missing.append("LANGFUSE_HOST")
        raise ValueError(f"Missing required Langfuse configuration: {', '.join(missing)}")

    logging.info(
        "Langfuse config loaded. Host: '%s', Public Key: %s, Secret Key: %s",
        host,
        _mask_secret(public_key),
        _mask_secret(secret_key),
    )

    return Langfuse(public_key=public_key, secret_key=secret_key, host=host)

def clean_langfuse_datasets(dataset_names: list):
    # Initialize the Langfuse client (reads credentials from environment variables)
    try:
        langfuse = _get_langfuse_client()
    except Exception as e:
        logging.error(f"Failed to initialize Langfuse client: {e}")
        return

    for dataset_name in dataset_names:
        logging.info(f"--- Starting cleanup for dataset: '{dataset_name}' ---")
        
        # ---------------------------------------------------------
        # Step 1: Fetch and Delete all Experiment Runs
        # ---------------------------------------------------------
        logging.info(f"Fetching experiment runs for '{dataset_name}'...")
        run_names = []
        try:
            # Fetch the first page of dataset runs
            runs_response = langfuse.api.datasets.get_runs(dataset_name=dataset_name, limit=100)
            if runs_response and runs_response.data:
                run_names = [run.name for run in runs_response.data]
        except Exception as e:
            logging.warning(f"Could not retrieve runs for dataset '{dataset_name}': {e}. Continuing...")

        if run_names:
            logging.info(f"Found {len(run_names)} runs to delete.")
            for run_name in run_names:
                try:
                    langfuse.api.datasets.delete_run(
                        dataset_name=dataset_name, 
                        run_name=run_name
                    )
                    logging.info(f"Successfully deleted run: '{run_name}'")
                except Exception as e:
                    logging.error(f"Failed to delete run '{run_name}': {e}")
        else:
            logging.info("No experiment runs found.")

        # ---------------------------------------------------------
        # Step 2: Fetch and Delete all Dataset Items
        # ---------------------------------------------------------
        logging.info(f"Fetching dataset items for '{dataset_name}'...")
        item_ids = []
        page = 1
        
        # We fetch all item IDs first to avoid pagination shift errors while deleting
        while True:
            try:
                items_response = langfuse.api.dataset_items.list(
                    dataset_name=dataset_name, 
                    page=page, 
                    limit=100
                )
                if not items_response or not items_response.data:
                    break
                
                for item in items_response.data:
                    item_ids.append(item.id)
                page += 1
            except Exception as e:
                logging.warning(f"Could not list items on page {page} for '{dataset_name}': {e}")
                break

        if item_ids:
            logging.info(f"Found {len(item_ids)} dataset items to delete.")
            for item_id in item_ids:
                try:
                    langfuse.api.dataset_items.delete(id=item_id)
                    logging.info(f"Successfully deleted item ID: {item_id}")
                except Exception as e:
                    logging.error(f"Failed to delete item ID {item_id}: {e}")
        else:
            logging.info("No dataset items found.")
            
        logging.info(f"Finished cleanup for dataset: '{dataset_name}'\n")

if __name__ == "__main__":
    clean_langfuse_datasets(DATASETS_TO_CLEAN)