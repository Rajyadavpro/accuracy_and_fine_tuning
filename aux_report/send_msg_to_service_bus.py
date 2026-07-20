import json
import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage

def send_demo_message():
    # 1. Read settings
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

    # 2. Send the message
    try:
        print(f"[*] Connecting to Service Bus queue: '{queue_name}'...")
        with ServiceBusClient.from_connection_string(conn_str) as client:
            with client.get_queue_sender(queue_name) as sender:
                
                # --- UPDATED PAYLOAD HERE ---
                demo_payload = {
                    "record_ids": ["16395", "16487"], 
                    "File name": [
                        "July_10th__2026_1---RD----556Pgs----Anshika--_19_docs__20260710053559350879_0.pdf", 
                        "July_10th__2026_1---RD----556Pgs----Anshika--_19_docs__20260710053611701857_0_veteran.pdf"
                    ], 
                    "Ground_truth": [
                        {"category": "VA_Rating_Decision", "subcategory": "", "is_correct": True}, 
                        {"category": "VA_Rating_Decision", "subcategory": "", "is_correct": True}
                    ], 
                    "source": "tabak", 
                    "environment": "Dev", 
                    "process_type": "FineTuning", 
                    "queued_at": "2026-07-20T11:19:23.300398+00:00"
                }
                
                message = ServiceBusMessage(json.dumps(demo_payload))
                sender.send_messages(message)
                print(f"[+] Successfully sent demo message to '{queue_name}'.")
    except Exception as e:
        print(f"[-] Failed to send message. Error: {e}")

if __name__ == "__main__":
    send_demo_message()