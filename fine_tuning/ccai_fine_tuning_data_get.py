"""
Audio Fine Tuning Data Push (CCAI)

Workflow:
    1. Load last processed blob name from ClickHouse checkpoint.
    2. List blobs in 'audio-and-transcripts-prod' container (lexicographic order).
    3. Filter to blobs with name > checkpoint (incremental).
    4. Chunk blob names (ids_per_message per message).
    5. Dispatch to Service Bus queue with process_type="FineTuning", source="CCAI".
    6. Save the last dispatched blob name as the new checkpoint.

Required env vars:
    CCAI_STORAGE_CONNECTION_STRING  - Azure Blob Storage connection string
    SERVICE_BUS_CONNECTION_STRING   - Azure Service Bus connection string
    SERVICE_BUS_QUEUE_NAME          - Queue name
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import MessageSizeExceededError

from clickhouse_store import (
    AUDIO_FINETUNING_CHECKPOINT_TABLE,
    get_environment,
    load_checkpoint_str,
    save_checkpoint_str,
)

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")

CONTAINER_NAME = "audio-and-transcripts-prod"


def _load_checkpoint() -> Optional[str]:
    """Load last processed blob name from ClickHouse."""
    try:
        env = get_environment()
        value = load_checkpoint_str(AUDIO_FINETUNING_CHECKPOINT_TABLE, env)
        if value:
            logging.info(f"[CCAI] Checkpoint loaded: '{value}'")
        else:
            logging.info("[CCAI] No checkpoint found. Processing all blobs.")
        return value
    except Exception as ex:
        logging.warning(f"[CCAI] Could not load checkpoint: {ex}. Starting clean.")
        return None


def _save_checkpoint(last_blob_name: str) -> None:
    """Save last processed blob name to ClickHouse."""
    try:
        env = get_environment()
        save_checkpoint_str(AUDIO_FINETUNING_CHECKPOINT_TABLE, env, last_blob_name)
        logging.info(f"[CCAI] Checkpoint saved: '{last_blob_name}'")
    except Exception as ex:
        logging.error(f"[CCAI] Failed to save checkpoint: {ex}")
        raise


def _list_blobs_after_checkpoint(
    connection_string: str, 
    checkpoint: Optional[str], 
    limit: Optional[int] = None
) -> List[str]:
    """List blob names from container lexicographically, starting after the checkpoint.
    
    Optimizations applied:
      1. Uses list_blob_names() instead of list_blobs() to fetch only string names (faster, less memory).
      2. Drops in-memory sorted() since Azure Storage results are already sorted lexicographically.
      3. Uses 'start_from' keyword (SDK v12.28.0+) to query directly from the checkpoint server-side.
      4. Breaks early once the requested 'limit' is reached to prevent over-fetching from Azure.
      5. Falls back gracefully to standard stream-filtering if 'start_from' is not supported by the local SDK.
    """
    logging.info(f"[CCAI] Listing blobs in '{CONTAINER_NAME}'...")
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    filtered_blobs: List[str] = []

    try:
        # Attempt server-side start location (azure-storage-blob >= 12.28.0)
        kwargs = {"start_from": checkpoint} if checkpoint else {}
        blob_iterator = container_client.list_blob_names(**kwargs)
        
        for name in blob_iterator:
            if not name:
                continue
            # Since 'start_from' is inclusive of the checkpoint name, we skip the exact match
            if checkpoint and name == checkpoint:
                continue
                
            filtered_blobs.append(name)
            
            # Stop pulling pages from Azure once the limit is fulfilled
            if limit is not None and len(filtered_blobs) >= limit:
                break
                
    except TypeError:
        # Fallback for older SDK versions that do not support the 'start_from' keyword
        logging.warning("[CCAI] 'start_from' keyword not supported by SDK. Falling back to stream filtering.")
        blob_iterator = container_client.list_blob_names()
        
        for name in blob_iterator:
            if not name:
                continue
            # Lexicographical skip
            if checkpoint and name <= checkpoint:
                continue
                
            filtered_blobs.append(name)
            
            if limit is not None and len(filtered_blobs) >= limit:
                break

    logging.info(f"[CCAI] Retrieved {len(filtered_blobs)} blob(s) for processing.")
    return filtered_blobs


def _send_to_queue(
    connection_string: str,
    queue_name: str,
    blob_chunks: List[List[str]],
) -> int:
    """Send chunked blob names to Service Bus using ServiceBusMessageBatch to avoid size limits."""
    logging.info(f"[CCAI] Sending {len(blob_chunks)} message(s) to queue '{queue_name}'...")
    sent = 0
    with ServiceBusClient.from_connection_string(connection_string) as sb_client:
        with sb_client.get_queue_sender(queue_name=queue_name) as sender:
            # Prepare message batch to ensure size constraints are respected safely
            batch_message = sender.create_message_batch()
            
            for chunk in blob_chunks:
                payload = {
                    "blob_names": chunk,
                    "container": CONTAINER_NAME,
                    "source": "CCAI",
                    "process_type": "FineTuning",
                    "queued_at": datetime.now(timezone.utc).isoformat(),
                }
                msg = ServiceBusMessage(json.dumps(payload))
                
                try:
                    batch_message.add_message(msg)
                    sent += 1
                except (ValueError, MessageSizeExceededError):
                    # Current batch is full. Send it, then start a new one to hold the message.
                    sender.send_messages(batch_message)
                    batch_message = sender.create_message_batch()
                    batch_message.add_message(msg)
                    sent += 1

            # Dispatch remaining messages in the final batch
            if len(batch_message) > 0:
                sender.send_messages(batch_message)

    logging.info(f"[CCAI] Successfully sent {sent} message(s).")
    return sent


def audio_fine_tuning_data_push(
    ids_per_message: Optional[int] = None,
    max_messages_per_run: Optional[int] = None,
) -> None:
    """Main entry point: fetch blobs, chunk, dispatch to queue, save checkpoint."""
    logging.info("=============================================================")
    logging.info("[CCAI] Starting Audio Fine Tuning Data Push...")
    logging.info("=============================================================")

    # --- Env vars ---
    # Aligned environment variable names with project requirements
    blob_conn_str = (
        os.getenv("CCAI_STORAGE_CONNECTION_STRING") or 
        os.getenv("AUDIO_BLOB_CONNECTION_STRING") or 
        ""
    ).strip()
    if not blob_conn_str:
        raise ValueError("CCAI_STORAGE_CONNECTION_STRING (or AUDIO_BLOB_CONNECTION_STRING) must be set.")

    sb_conn_str = os.getenv("SERVICE_BUS_CONNECTION_STRING", "").strip()
    if not sb_conn_str:
        raise ValueError("SERVICE_BUS_CONNECTION_STRING must be set.")

    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
    if not queue_name:
        raise ValueError("SERVICE_BUS_QUEUE_NAME must be set.")

    # --- Effective batch params ---
    eff_ids_per_message = ids_per_message if ids_per_message and ids_per_message > 0 else int(os.getenv("IDS_PER_MESSAGE", "10") or "10")
    if max_messages_per_run is not None:
        eff_max_messages = max_messages_per_run
    else:
        raw = (os.getenv("MAX_MESSAGES_PER_RUN") or "").strip()
        eff_max_messages = int(raw) if raw else None

    # Calculate optimal limit to query only what is needed
    limit = None
    if eff_max_messages is not None:
        limit = eff_max_messages * eff_ids_per_message

    logging.info(f"[CCAI] ids_per_message={eff_ids_per_message}, max_messages_per_run={eff_max_messages}, query_limit={limit}")

    # --- Step 1: Checkpoint ---
    checkpoint = _load_checkpoint()

    # --- Step 2: List blobs with early-stopping and server-side paging ---
    blobs = _list_blobs_after_checkpoint(blob_conn_str, checkpoint, limit=limit)
    if not blobs:
        logging.info("[CCAI] No new blobs to process. Done.")
        return

    # --- Step 3: Chunk ---
    chunks = [blobs[i:i + eff_ids_per_message] for i in range(0, len(blobs), eff_ids_per_message)]
    if eff_max_messages is not None and len(chunks) > eff_max_messages:
        chunks = chunks[:eff_max_messages]

    last_blob_name = chunks[-1][-1]

    # --- Step 4: Dispatch ---
    _send_to_queue(sb_conn_str, queue_name, chunks)

    # --- Step 5: Save checkpoint ---
    _save_checkpoint(last_blob_name)

    logging.info("=============================================================")
    logging.info(f"[CCAI] Done. {len(chunks)} message(s) sent. Last blob checkpoint: '{last_blob_name}'")
    logging.info("=============================================================")


if __name__ == "__main__":
    audio_fine_tuning_data_push()