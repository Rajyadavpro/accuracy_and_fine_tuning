#!/usr/bin/env python3
"""Send EOB fine-tuning records from file to Service Bus queue."""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# Load settings
settings_path = Path(__file__).parent / "local.settings.json"
if settings_path.exists():
    with open(settings_path) as f:
        settings = json.load(f)
        for k, v in settings["Values"].items():
            os.environ[k] = str(v)

queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "ai-accuarcy-and-fine-tuning").strip()
conn_string = os.getenv("SERVICE_BUS_CONNECTION_STRING", "").strip()
eob_file = Path(__file__).parent / "fine_tuning/EOB_Fine_Tuning_data/eob_fine_tuning_20260712.json"

if not eob_file.exists():
    print(f"ERROR: File not found: {eob_file}")
    sys.exit(1)

if not conn_string:
    print("ERROR: SERVICE_BUS_CONNECTION_STRING not set")
    sys.exit(1)

# Load records
print(f"[*] Loading records from {eob_file.name}...")
with open(eob_file) as f:
    records = json.load(f)

print(f"[*] Total records: {len(records)}")
print(f"[*] Connecting to queue '{queue_name}'...")

# Send to queue with retries and batching
try:
    messages_sent = 0
    batch_size = 10
    max_retries = 3
    
    for batch_start in range(0, len(records), batch_size):
        batch_end = min(batch_start + batch_size, len(records))
        batch = records[batch_start:batch_end]
        
        for attempt in range(max_retries):
            try:
                client = ServiceBusClient.from_connection_string(conn_string)
                
                with client.get_queue_sender(queue_name) as sender:
                    for record in batch:
                        body = {
                            "file_name": record.get("file_name"),
                            "allocation_id": record.get("allocation_id"),
                            "ground_truth": record.get("ground_truth"),
                            "source": "healthcare_eob",
                            "environment": os.getenv("LANGFUSE_ENVIRONMENT", "exp"),
                            "process_type": "FineTuning",
                            "queued_at": datetime.now(timezone.utc).isoformat(),
                        }
                        msg = ServiceBusMessage(json.dumps(body))
                        sender.send_messages(msg)
                        messages_sent += 1
                
                print(f"[*] Sent batch {batch_start}-{batch_end-1} ({batch_end}/{len(records)} total)")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # exponential backoff
                    print(f"[!] Batch {batch_start}-{batch_end-1} failed (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    print(f"[-] Batch {batch_start}-{batch_end-1} failed after {max_retries} attempts: {e}")
                    # Continue with next batch instead of failing
                    break
    
    print(f"[+] SUCCESS: {messages_sent} records sent to queue '{queue_name}'")
except Exception as e:
    print(f"[-] FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
