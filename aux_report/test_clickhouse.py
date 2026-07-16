#!/usr/bin/env python3
"""
Simple test to send sample data to ClickHouse and verify connectivity.
"""

import sys
import csv
import json
from datetime import datetime
from clickhouse_driver import Client

def test_clickhouse_connection():
    """Test basic ClickHouse connectivity."""
    print("=" * 80)
    print("[TEST] ClickHouse Connection Test")
    print("=" * 80)
    
    try:
        print("[INFO] Connecting to ClickHouse at 172.173.148.33:9000...")
        client = Client(
            host='172.173.148.33',
            port=9000,
            user='admin',
            password='Holly7583hfxZ',
            database='accuracy_and_finetuning',
            connect_timeout=10
        )
        
        print("[✓] Connection established successfully!")
        
        # Test database
        print("\n[INFO] Testing database connection...")
        result = client.execute("SELECT 1 as test")
        print(f"[✓] Database query successful: {result}")
        
        return client
        
    except Exception as e:
        print(f"[✗] Connection failed: {e}")
        return None


def load_sample_data():
    """Load sample data from CSV."""
    print("\n[INFO] Loading sample data from sample_records.csv...")
    
    records = []
    try:
        with open('sample_records.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 5:  # Just take first 5 records
                    break
                records.append(row)
        
        print(f"[✓] Loaded {len(records)} sample records")
        return records
        
    except Exception as e:
        print(f"[✗] Failed to load CSV: {e}")
        return []


def parse_predicted_category(response_payload):
    """Extract Predicted Category from ResponsePayload JSON."""
    try:
        data = json.loads(response_payload)
        if 'json' in data and len(data['json']) > 0:
            return data['json'][0].get('Predicted Category', 'Unknown')
    except:
        pass
    return 'Unknown'


def insert_test_data(client, records):
    """Insert test data into ClickHouse."""
    if not client or not records:
        return False
    
    print("\n[INFO] Preparing test data for insertion...")
    
    try:
        # Create test table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_idp_data (
            id UInt32,
            batch_id String,
            client_code String,
            predicted_category String,
            created_on DateTime,
            file_name String,
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (created_on, batch_id)
        """
        
        print("[INFO] Creating test table...")
        client.execute(create_table_sql)
        print("[✓] Test table created/verified")
        
        # Prepare data for insertion
        rows_to_insert = []
        for record in records:
            try:
                created_on = datetime.strptime(record['CreatedOn'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                created_on = datetime.now()
            
            predicted_category = parse_predicted_category(record['ResponsePayload'])
            
            rows_to_insert.append([
                int(record['ID']),
                record['BatchId'],
                record.get('ResponsePayload', '')[:50],  # Extract client code hint
                predicted_category,
                created_on,
                record.get('TransactionId', '')[:30],
            ])
        
        print(f"\n[INFO] Inserting {len(rows_to_insert)} records...")
        
        # Insert data
        client.execute(
            'INSERT INTO test_idp_data (id, batch_id, client_code, predicted_category, created_on, file_name) VALUES',
            rows_to_insert
        )
        
        print(f"[✓] Successfully inserted {len(rows_to_insert)} records!")
        
        # Verify insertion
        print("\n[INFO] Verifying inserted data...")
        result = client.execute("SELECT COUNT(*) as count FROM test_idp_data")
        count = result[0][0] if result else 0
        print(f"[✓] Table now contains {count} records")
        
        # Show sample records
        print("\n[INFO] Sample data from table:")
        print("-" * 80)
        sample = client.execute("""
            SELECT 
                id,
                batch_id,
                predicted_category,
                created_on,
                inserted_at
            FROM test_idp_data
            LIMIT 5
        """)
        
        for row in sample:
            print(f"  ID: {row[0]}, Batch: {row[1]}, Category: {row[2]}, Created: {row[3]}")
        
        return True
        
    except Exception as e:
        print(f"[✗] Insert failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("\n[START] ClickHouse Test")
    print(f"[TIME] {datetime.now()}\n")
    
    # Step 1: Connect
    client = test_clickhouse_connection()
    if not client:
        print("\n[FAILED] Could not connect to ClickHouse")
        return 1
    
    # Step 2: Load data
    records = load_sample_data()
    if not records:
        print("\n[FAILED] Could not load sample data")
        return 1
    
    # Step 3: Insert data
    success = insert_test_data(client, records)
    
    print("\n" + "=" * 80)
    if success:
        print("[SUCCESS] ClickHouse is working! Data inserted and verified.")
        print("=" * 80)
        return 0
    else:
        print("[FAILED] ClickHouse test did not complete")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
