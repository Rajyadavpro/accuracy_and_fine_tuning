import json
import os
import time
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode

def fetch_and_save_messages(limit=None, output_dir="fetched_messages", delete_from_queue=True):
    # 1. Read settings from local.settings.json
    if not os.path.exists("local.settings.json"):
        print("[-] local.settings.json not found.")
        return

    with open("local.settings.json", "r") as f:
        settings = json.load(f).get("Values", {})

    conn_str = settings.get("SERVICE_BUS_CONNECTION_STRING")
    queue_name = settings.get("SERVICE_BUS_QUEUE_NAME")

    if not conn_str or "your-namespace" in conn_str:
        print("[-] Please replace the placeholder in SERVICE_BUS_CONNECTION_STRING with your actual connection string.")
        return

    # Ensure the local output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # 2. Determine if we have a valid numerical limit
    max_limit = None
    if limit is not None:
        try:
            parsed_limit = int(limit)
            if parsed_limit > 0:
                max_limit = parsed_limit
            else:
                print("[*] Limit is 0 or negative. Falling back to fetching ALL messages.")
        except (ValueError, TypeError):
            print(f"[*] Limit '{limit}' is invalid. Falling back to fetching ALL messages.")
            max_limit = None

    # Print mode configuration
    mode_str = "FETCH & DELETE" if delete_from_queue else "PEEK ONLY (Read-Only, Keeps messages in queue)"
    limit_str = f"up to {max_limit}" if max_limit is not None else "ALL"
    print(f"[*] Mode: {mode_str}")
    print(f"[*] Connecting to queue '{queue_name}' to retrieve {limit_str} messages...")

    fetched_count = 0
    from_sequence = 1  # Track starting sequence number for read-only peeking

    try:
        with ServiceBusClient.from_connection_string(conn_str) as client:
            with client.get_queue_receiver(queue_name, receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
                while True:
                    # Calculate how many messages to request in this batch
                    if max_limit is not None:
                        remaining = max_limit - fetched_count
                        if remaining <= 0:
                            break
                        batch_size = min(remaining, 100)  # Maximum 100 per request
                    else:
                        batch_size = 100

                    # 3. Retrieve messages based on toggle
                    if delete_from_queue:
                        # Locks and prepares messages for completion/deletion
                        messages = receiver.receive_messages(max_message_count=batch_size, max_wait_time=5)
                    else:
                        # Safely browses the queue starting at from_sequence without locking or modifying
                        messages = receiver.peek_messages(max_message_count=batch_size, sequence_number=from_sequence)

                    if not messages:
                        if delete_from_queue:
                            print("[*] No more messages found in the queue (waited 5 seconds).")
                        else:
                            print("[*] Reached the end of the queue (no more messages to peek).")
                        break

                    for msg in messages:
                        # Extract metadata
                        msg_id = str(msg.message_id) if msg.message_id else f"gen_{fetched_count}_{int(time.time())}"
                        seq_num = msg.sequence_number
                        enqueued_time = str(msg.enqueued_time_utc) if msg.enqueued_time_utc else None
                        
                        # Extract and parse body
                        raw_body = str(msg)
                        try:
                            body_content = json.loads(raw_body)
                        except json.JSONDecodeError:
                            body_content = raw_body

                        # Compile local file payload
                        saved_payload = {
                            "metadata": {
                                "message_id": msg_id,
                                "sequence_number": seq_num,
                                "enqueued_time_utc": enqueued_time
                            },
                            "body": body_content
                        }

                        # Define filename based on sequence number or message ID
                        filename = f"msg_{seq_num or msg_id}.json"
                        file_path = os.path.join(output_dir, filename)

                        # Save payload to local disk
                        with open(file_path, "w", encoding="utf-8") as out_f:
                            json.dump(saved_payload, out_f, indent=4)

                        # 4. Handle message retention toggle
                        if delete_from_queue:
                            # Settle message to remove it from the queue
                            receiver.complete_message(msg)
                        else:
                            # Advance the browse cursor so the next iteration doesn't read the same messages
                            from_sequence = seq_num + 1
                        
                        fetched_count += 1
                        
                        if max_limit is not None:
                            print(f"[+] Saved message {fetched_count}/{max_limit} to '{file_path}'")
                            if fetched_count >= max_limit:
                                break
                        else:
                            print(f"[+] Saved message {fetched_count} to '{file_path}'")

                    # Break the outer while loop if we have reached our limit
                    if max_limit is not None and fetched_count >= max_limit:
                        break
                        
        action_word = "fetched and deleted" if delete_from_queue else "peeked and saved (retained in queue)"
        print(f"\n[+] Completed. Successfully {action_word} {fetched_count} messages locally to '{output_dir}/'.")
        
    except Exception as e:
        print(f"[-] Error: {e}")

if __name__ == "__main__":
    # --- TOGGLE TEST CASES ---
    # N = 5           # Will download up to 5 messages
    # N = None        # Will download ALL messages
    
    N = None 
    
    # --- DELETION TOGGLE ---
    # True  -> Messages are downloaded AND deleted from the Azure Queue.
    # False -> Messages are downloaded, but remain in the Queue (Read-Only / Peek mode).
    DELETE_FROM_QUEUE = False
    
    fetch_and_save_messages(limit=N, delete_from_queue=DELETE_FROM_QUEUE, output_dir=r"C:\Users\raj.kumaryadav\Desktop\Superbill\Main_Git_repo\Accuracy_and_Fine_tuning_f1\fetched_messages")