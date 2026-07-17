#!/usr/bin/env python3
import requests
from clickhouse_store import get_environment, load_checkpoint_str, AUDIO_FINETUNING_CHECKPOINT_TABLE

try:
    env = get_environment()
    checkpoint = load_checkpoint_str(AUDIO_FINETUNING_CHECKPOINT_TABLE, env)
    print(f"Current CCAI checkpoint: '{checkpoint}'")
    
    # Check if it's a UUID (36 chars with dashes) or a full blob path (contains /)
    if checkpoint:
        is_uuid = len(checkpoint) == 36 and checkpoint.count('-') == 4 and '/' not in checkpoint
        is_blob_path = '/' in checkpoint
        print(f"Checkpoint type: {'UUID' if is_uuid else 'Full blob path' if is_blob_path else 'Unknown'}")
    else:
        print("No checkpoint found")
except Exception as e:
    print(f"Error: {e}")
