import json
import os
import time
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode

def fetch_and_save_messages(num_messages=10, output_dir="fetched_messages"):
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

    print(f"[*] Connecting to queue '{queue_name}' to fetch up to {num_messages} messages...")
    fetched_count = 0

    try:
        with ServiceBusClient.from_connection_string(conn_str) as client:
            # PEEK_LOCK mode lets us lock the message, process/save it, and complete it afterward
            with client.get_queue_receiver(queue_name, receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
                while fetched_count < num_messages:
                    remaining = num_messages - fetched_count
                    batch_size = min(remaining, 10)  # Fetch in batches of up to 10
                    
                    # Fetch messages with a 5-second wait timeout if the queue is empty
                    messages = receiver.receive_messages(max_message_count=batch_size, max_wait_time=5)
                    if not messages:
                        print("[*] No more messages found in the queue.")
                        break

                    for msg in messages:
                        # Extract metadata
                        msg_id = str(msg.message_id) if msg.message_id else f"gen_{fetched_count}_{int(time.time())}"
                        seq_num = msg.sequence_number
                        enqueued_time = str(msg.enqueued_time_utc) if msg.enqueued_time_utc else None
                        
                        # Extract and parse body (handling JSON bodies or falling back to raw string)
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

                        # Settle message (complete it to remove it from the queue)
                        receiver.complete_message(msg)
                        
                        fetched_count += 1
                        print(f"[+] Saved message {fetched_count}/{num_messages} to '{file_path}'")
                        
        print(f"\n[+] Completed. Successfully fetched and saved {fetched_count} messages locally to '{output_dir}/'.")
        
    except Exception as e:
        print(f"[-] Error fetching messages: {e}")

if __name__ == "__main__":
    # You can change the number of messages to fetch here
    N = 10 
    fetch_and_save_messages(num_messages=N)