#!/usr/bin/env python3
"""
Minimal test to verify Service Bus send_messages() works correctly.
Tests ONLY the send operation with a small batch.
"""
import sys
import os
import json
from datetime import datetime, timezone

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load local.settings.json
try:
    with open("local.settings.json") as f:
        settings = json.load(f)
        for key, value in settings.get("Values", {}).items():
            if key not in os.environ:
                os.environ[key] = value
except Exception as e:
    print(f"[WARNING] Could not load local.settings.json: {e}")

from azure.servicebus import ServiceBusClient, ServiceBusMessage

def test_send_messages():
    """Test sending a small batch of messages to Service Bus."""
    
    # Get connection string
    conn_str = os.getenv("SERVICE_BUS_CONNECTION_STRING")
    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "ai-accuarcy-and-fine-tuning")
    
    if not conn_str:
        print("[ERROR] SERVICE_BUS_CONNECTION_STRING not set")
        print(f"[DEBUG] Available env vars: {list(os.environ.keys())}")
        return False
    
    print(f"[TEST] Testing Service Bus send with queue: {queue_name}")
    print(f"[TEST] Creating 5 test messages...")
    
    test_blob_batches = [
        [f"test-blob-{i}.wav" for i in range(1, 4)],
        [f"test-blob-{i}.wav" for i in range(4, 7)],
        [f"test-blob-{i}.wav" for i in range(7, 10)],
        [f"test-blob-{i}.wav" for i in range(10, 13)],
        [f"test-blob-{i}.wav" for i in range(13, 16)],
    ]
    
    sb_client = None
    sender = None
    
    try:
        # Create client
        print("[TEST] Creating ServiceBusClient...")
        sb_client = ServiceBusClient.from_connection_string(
            conn_str,
            http_request_timeout=30,
            operation_timeout=30
        )
        print("[TEST] ✓ ServiceBusClient created")
        
        # Get sender
        print(f"[TEST] Getting sender for queue '{queue_name}'...")
        sender = sb_client.get_queue_sender(queue_name=queue_name)
        print("[TEST] ✓ Sender obtained")
        
        # Send messages one at a time to isolate the issue
        for idx, blob_batch in enumerate(test_blob_batches):
            print(f"\n[TEST] Sending batch {idx + 1}/5 ({len(blob_batch)} blobs)...")
            
            try:
                payload = {
                    "blob_names": blob_batch,
                    "container": "audio-and-transcripts-prod",
                    "folder_name": "test",
                    "source": "TEST",
                    "process_type": "FineTuning",
                    "queued_at": datetime.now(timezone.utc).isoformat(),
                }
                
                msg = ServiceBusMessage(json.dumps(payload))
                print(f"  [TEST] Message created, size: {len(json.dumps(payload))} bytes")
                
                # Send message
                print(f"  [TEST] Calling sender.send_messages()...")
                sender.send_messages(msg)
                print(f"  [TEST] ✓ Batch {idx + 1} sent successfully")
                
            except Exception as e:
                print(f"  [ERROR] Failed to send batch {idx + 1}: {e}")
                import traceback
                traceback.print_exc()
                return False
        
        print("\n[SUCCESS] All 5 test batches sent successfully!")
        return True
        
    except Exception as e:
        print(f"[ERROR] Critical failure: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if sender:
            try:
                sender.close()
                print("[TEST] Sender closed")
            except Exception as e:
                print(f"[WARNING] Error closing sender: {e}")
        
        if sb_client:
            try:
                sb_client.close()
                print("[TEST] ServiceBusClient closed")
            except Exception as e:
                print(f"[WARNING] Error closing client: {e}")


if __name__ == "__main__":
    success = test_send_messages()
    sys.exit(0 if success else 1)
