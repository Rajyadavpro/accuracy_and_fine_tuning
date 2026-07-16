import json
import os
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode

def purge_service_bus_queue():
    if not os.path.exists("local.settings.json"):
        print("[-] local.settings.json not found.")
        return

    with open("local.settings.json", "r") as f:
        settings = json.load(f).get("Values", {})

    conn_str = settings.get("SERVICE_BUS_CONNECTION_STRING")
    queue_name = settings.get("SERVICE_BUS_QUEUE_NAME")

    if not conn_str or "your-namespace" in conn_str:
        print("[-] Please ensure your actual SERVICE_BUS_CONNECTION_STRING is in local.settings.json.")
        return

    try:
        print(f"[*] Starting to clear messages from '{queue_name}'...")
        total_purged = 0
        
        # Connect in RECEIVE_AND_DELETE mode to pull and delete instantly
        with ServiceBusClient.from_connection_string(conn_str) as client:
            with client.get_queue_receiver(queue_name, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as receiver:
                while True:
                    # Receive in batches of 100 with a 3-second timeout limit when empty
                    messages = receiver.receive_messages(max_message_count=1000, max_wait_time=3)
                    if not messages:
                        break # Stopped receiving messages, queue is empty
                    
                    total_purged += len(messages)
                    print(f"[*] Cleared {len(messages)} messages (Running Total: {total_purged})...")
                    
        print(f"[+] Completed. Purged {total_purged} messages from '{queue_name}'.")
    except Exception as e:
        print(f"[-] Error purging Service Bus queue: {e}")

if __name__ == "__main__":
    purge_service_bus_queue()