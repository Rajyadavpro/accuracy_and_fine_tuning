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


def _extract_uuid_from_blob(blob_name: str) -> str:
    """Extract UUID (folder part) from blob name.
    
    Example: "00077591-0030-4fa3-aa18-924802d05403/EG_cwLKpmHb8ELi.json" -> "00077591-0030-4fa3-aa18-924802d05403"
    """
    if '/' in blob_name:
        return blob_name.split('/', 1)[0]
    return blob_name


def _load_checkpoint() -> Optional[str]:
    """Load last processed UUID from ClickHouse (not filename)."""
    try:
        env = get_environment()
        value = load_checkpoint_str(AUDIO_FINETUNING_CHECKPOINT_TABLE, env)
        if value:
            logging.info(f"[CCAI] Checkpoint loaded UUID: '{value}'")
        else:
            logging.info("[CCAI] No checkpoint found. Processing all UUID folders.")
        return value
    except Exception as ex:
        logging.warning(f"[CCAI] Could not load checkpoint: {ex}. Starting clean.")
        return None


def _save_checkpoint(last_uuid: str) -> None:
    """Save last processed UUID to ClickHouse."""
    try:
        env = get_environment()
        save_checkpoint_str(AUDIO_FINETUNING_CHECKPOINT_TABLE, env, last_uuid)
        logging.info(f"[CCAI] Checkpoint saved UUID: '{last_uuid}'")
    except Exception as ex:
        logging.error(f"[CCAI] Failed to save checkpoint: {ex}")
        raise


def _list_blobs_after_checkpoint(
    connection_string: str, 
    checkpoint_uuid: Optional[str], 
    limit: Optional[int] = None
) -> dict[str, List[str]]:
    """List blobs grouped by UUID, filtering to UUID folders after checkpoint.
    
    Returns a dict: {uuid: [blob_names]}
    Example: {"00077591-0030-4fa3-aa18-924802d05403": ["...file1.json", "...file2.json"], ...}
    
    Optimizations applied:
      1. Uses list_blob_names() instead of list_blobs() to fetch only string names (faster, less memory).
      2. Groups blobs by UUID (folder part before first '/').
      3. Filters to UUIDs > checkpoint (lexicographically).
      4. Breaks early once enough UUIDs are collected (limit is interpreted as UUID count, not blob count).
    
    Args:
        connection_string: Azure Storage connection string
        checkpoint_uuid: Last processed UUID (exclusive)
        limit: Max UUIDs to retrieve (not blob count!). 
               Formula: If limit=6 blobs requested for 1 msg with 2 UUIDs per msg,
               we convert to uuid_limit=1 UUID minimum, but fetch with safety multiplier.
    """
    logging.info(f"[CCAI] Listing blobs in '{CONTAINER_NAME}' grouped by UUID...")
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    uuid_groups: dict[str, List[str]] = {}
    total_blobs = 0
    total_uuids = 0
    
    # Convert blob limit to UUID limit
    # If blob_limit=6 and avg 3 blobs per UUID, we want at least 2 UUIDs
    # Add safety multiplier to ensure we get the full UUIDs
    uuid_limit = None
    blob_fetch_limit = None
    if limit is not None:
        uuid_limit = max(1, limit // 3)  # Assume ~3 blobs per UUID on average
        blob_fetch_limit = limit * 10     # Fetch up to 10x to ensure we get full UUIDs

    try:
        # Attempt server-side start location (azure-storage-blob >= 12.28.0)
        # Start from checkpoint UUID to reduce over-fetching
        kwargs = {"start_from": checkpoint_uuid} if checkpoint_uuid else {}
        blob_iterator = container_client.list_blob_names(**kwargs)
        
        for name in blob_iterator:
            if not name:
                continue
            
            uuid = _extract_uuid_from_blob(name)
            
            # Skip the checkpoint UUID itself (we want UUIDs AFTER it)
            if checkpoint_uuid and uuid == checkpoint_uuid:
                continue
            
            # Skip UUIDs that are lexicographically <= checkpoint
            if checkpoint_uuid and uuid <= checkpoint_uuid:
                continue
            
            # Track when we see a new UUID
            is_new_uuid = uuid not in uuid_groups
            
            # Group blob by UUID
            if is_new_uuid:
                uuid_groups[uuid] = []
                total_uuids += 1
                logging.debug(f"[CCAI] New UUID #{total_uuids}: {uuid}")
            
            uuid_groups[uuid].append(name)
            total_blobs += 1
            
            # Stop if we have enough UUIDs
            if uuid_limit is not None and total_uuids >= uuid_limit:
                logging.info(f"[CCAI] Reached UUID limit ({uuid_limit}), stopping blob enumeration")
                break
            
            # Also stop if we exceed blob fetch limit as safety net
            if blob_fetch_limit is not None and total_blobs >= blob_fetch_limit:
                logging.info(f"[CCAI] Reached blob fetch limit ({blob_fetch_limit}), stopping blob enumeration")
                break
                
    except TypeError:
        # Fallback for older SDK versions that do not support the 'start_from' keyword
        logging.warning("[CCAI] 'start_from' keyword not supported by SDK. Falling back to stream filtering.")
        blob_iterator = container_client.list_blob_names()
        
        for name in blob_iterator:
            if not name:
                continue
            
            uuid = _extract_uuid_from_blob(name)
            
            # Skip checkpoint UUID and earlier UUIDs
            if checkpoint_uuid and uuid <= checkpoint_uuid:
                continue
            
            # Track when we see a new UUID
            is_new_uuid = uuid not in uuid_groups
            
            if is_new_uuid:
                uuid_groups[uuid] = []
                total_uuids += 1
                logging.debug(f"[CCAI] New UUID #{total_uuids}: {uuid}")
            
            uuid_groups[uuid].append(name)
            total_blobs += 1
            
            # Stop if we have enough UUIDs
            if uuid_limit is not None and total_uuids >= uuid_limit:
                logging.info(f"[CCAI] Reached UUID limit ({uuid_limit}), stopping blob enumeration")
                break
            
            # Also stop if we exceed blob fetch limit as safety net
            if blob_fetch_limit is not None and total_blobs >= blob_fetch_limit:
                logging.info(f"[CCAI] Reached blob fetch limit ({blob_fetch_limit}), stopping blob enumeration")
                break

    logging.info(f"[CCAI] Retrieved {total_blobs} blob(s) in {len(uuid_groups)} UUID folder(s).")
    return uuid_groups


def _send_to_queue(
    connection_string: str,
    queue_name: str,
    blob_chunks: List[List[str]],
) -> int:
    """Send chunked blob names to Service Bus using ServiceBusMessageBatch to avoid size limits.
    
    Uses ServiceBusClient with extended socket timeout (30s) to handle cold-start latency
    from local execution. SDK has built-in retry logic for transient AMQP connection errors.
    """
    logging.info(f"[CCAI] Sending {len(blob_chunks)} message(s) to queue '{queue_name}'...")
    sent = 0
    # Set socket timeout to 30s for initial connection (cold start can be slow)
    with ServiceBusClient.from_connection_string(connection_string, max_wait_time=30) as sb_client:
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
    """Main entry point: fetch blobs grouped by UUID, dispatch to queue, save checkpoint."""
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
    # Each UUID might have multiple blobs, so request more blobs than messages needed
    limit = None
    if eff_max_messages is not None:
        # Request 3x the blobs in case UUIDs have multiple files
        limit = eff_max_messages * eff_ids_per_message * 3

    logging.info(f"[CCAI] ids_per_message={eff_ids_per_message}, max_messages_per_run={eff_max_messages}, query_limit={limit}")

    # --- Step 1: Checkpoint (UUID) ---
    checkpoint_uuid = _load_checkpoint()

    # --- Step 2: List blobs grouped by UUID ---
    uuid_groups = _list_blobs_after_checkpoint(blob_conn_str, checkpoint_uuid, limit=limit)
    if not uuid_groups:
        logging.info("[CCAI] No new UUID folders to process. Done.")
        return

    # --- Step 3: Process UUID groups ---
    # Get ordered list of UUIDs (preserve order from Azure Storage)
    ordered_uuids = list(uuid_groups.keys())
    logging.info(f"[CCAI] Processing {len(ordered_uuids)} UUID folder(s): {', '.join(ordered_uuids[:5])}{'...' if len(ordered_uuids) > 5 else ''}")
    
    # Batch UUIDs according to ids_per_message (e.g., 2 UUIDs per message)
    chunks = []
    current_chunk = []
    last_uuid_processed = None
    
    for uuid in ordered_uuids:
        # Check if we've reached max message limit
        if eff_max_messages is not None and len(chunks) >= eff_max_messages:
            break
        
        current_chunk.append(uuid)
        
        # When chunk reaches desired size, add it to chunks
        if len(current_chunk) >= eff_ids_per_message:
            chunks.append(current_chunk)
            last_uuid_processed = current_chunk[-1]
            current_chunk = []
    
    # Add any remaining UUIDs as final chunk
    if current_chunk:
        chunks.append(current_chunk)
        last_uuid_processed = current_chunk[-1]
    
    logging.info(f"[CCAI] Created {len(chunks)} message(s) from {len(ordered_uuids)} UUID(s)")
    
    if not chunks:
        logging.info("[CCAI] No blobs to process after filtering. Done.")
        return

    # --- Step 4: Dispatch ---
    _send_to_queue(sb_conn_str, queue_name, chunks)

    # --- Step 5: Save checkpoint (UUID) ---
    # Only update checkpoint after messages are successfully sent.
    # If checkpoint save fails here, messages are already in queue but checkpoint isn't updated,
    # causing reprocessing on next run (acceptable - data is idempotent, blobs can be reprocessed)
    if last_uuid_processed:
        try:
            _save_checkpoint(last_uuid_processed)
        except Exception as ex:
            logging.warning(f"[CCAI] Failed to save checkpoint '{last_uuid_processed}': {ex}. "
                           f"Messages already sent. Reprocessing on next run may occur.")
            # Don't re-raise - prefer to complete the function since messages are already sent

    logging.info("=============================================================")
    logging.info(f"[CCAI] Done. {len(chunks)} message(s) sent. Last UUID checkpoint: '{last_uuid_processed}'")
    logging.info("=============================================================")


if __name__ == "__main__":
    audio_fine_tuning_data_push()