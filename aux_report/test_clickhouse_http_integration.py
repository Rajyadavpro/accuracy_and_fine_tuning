#!/usr/bin/env python3
"""
Comprehensive ClickHouse HTTP Interface Test
Tests all functions from clickhouse_http_store.py
Verifies data can be inserted and queried successfully
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent / "accuracy"))

from clickhouse_http_store import (
    test_clickhouse_connection,
    ensure_database_and_table,
    insert_idp_transactions_http,
    load_idp_accuracy_checkpoint,
    get_environment,
    logger
)

def print_section(title: str):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")

def test_phase_1_connection():
    print_section("PHASE 1: Test ClickHouse HTTP Connection")
    
    print("[TEST] Checking ClickHouse HTTP interface on port 8123...")
    result = test_clickhouse_connection(timeout=10)
    
    if result:
        print("✅ PASS: ClickHouse HTTP interface is responsive")
        return True
    else:
        print("❌ FAIL: Cannot connect to ClickHouse HTTP interface")
        print("   Verify: http://172.173.148.33:8123 is accessible")
        print("   Check: ClickHouse HTTP service is running")
        return False

def test_phase_2_database_init():
    print_section("PHASE 2: Initialize Database & Table Structure")
    
    print(f"[TEST] Creating database and table structure...")
    print(f"[INFO] Environment: {get_environment()}")
    print(f"[INFO] Database: accuracy_and_finetuning")
    print(f"[INFO] Table: idp_accuracy_transactions")
    
    result = ensure_database_and_table(timeout=10)
    
    if result:
        print("✅ PASS: Database and table structure ready")
        return True
    else:
        print("❌ FAIL: Could not initialize database or table")
        return False

def test_phase_3_insert_sample_data():
    print_section("PHASE 3: Insert Sample Data")
    
    sample_records = [
        {
            "BatchId": "BATCH-TEST-001",
            "CreatedOn": datetime(2026, 7, 16, 10, 30, 0),
            "Filename": "test_invoice_001.pdf",
            "ClientCode": "CLIENT_TEST_A",
            "PredictedCategory": "Invoice"
        },
        {
            "BatchId": "BATCH-TEST-001",
            "CreatedOn": datetime(2026, 7, 16, 10, 35, 0),
            "Filename": "test_receipt_001.pdf",
            "ClientCode": "CLIENT_TEST_A",
            "PredictedCategory": "Receipt"
        },
        {
            "BatchId": "BATCH-TEST-002",
            "CreatedOn": datetime(2026, 7, 16, 11, 0, 0),
            "Filename": "test_doc_001.pdf",
            "ClientCode": "CLIENT_TEST_B",
            "PredictedCategory": "Unknown"
        },
        {
            "BatchId": "BATCH-TEST-002",
            "CreatedOn": datetime(2026, 7, 16, 11, 15, 0),
            "Filename": "test_contract_001.pdf",
            "ClientCode": "CLIENT_TEST_B",
            "PredictedCategory": "Contract"
        },
        {
            "BatchId": "BATCH-TEST-003",
            "CreatedOn": datetime(2026, 7, 16, 12, 0, 0),
            "Filename": "test_blank_scan.pdf",
            "ClientCode": "CLIENT_TEST_C",
            "PredictedCategory": ""
        }
    ]
    
    print(f"[TEST] Inserting {len(sample_records)} test records...")
    for idx, record in enumerate(sample_records, 1):
        print(f"  Record {idx}: {record['Filename']} ({record['ClientCode']})")
    
    result = insert_idp_transactions_http(
        environment=get_environment(),
        records=sample_records,
        checkpoint_datetime=datetime.utcnow(),
        timeout=15
    )
    
    if result:
        print(f"✅ PASS: Successfully inserted {len(sample_records)} test records")
        return True, len(sample_records)
    else:
        print(f"❌ FAIL: Could not insert test records")
        return False, 0

def test_phase_4_verify_data():
    print_section("PHASE 4: Verify Inserted Data")
    
    print("[TEST] Checking if test records are in ClickHouse...")
    print("[INFO] Querying table for records with BATCH-TEST batch IDs")
    
    import requests
    from clickhouse_http_store import _get_clickhouse_config
    
    try:
        host, http_port, database, user, password = _get_clickhouse_config()
        
        query = f"""
        SELECT COUNT(*) as record_count, 
               COUNT(DISTINCT BatchId) as unique_batches,
               COUNT(DISTINCT ClientCode) as unique_clients
        FROM `{database}`.`idp_accuracy_transactions`
        WHERE BatchId LIKE 'BATCH-TEST%'
        """
        
        response = requests.get(
            f"http://{host}:{http_port}/",
            auth=(user, password),
            timeout=10,
            params={"query": query}
        )
        
        if response.status_code == 200:
            result_text = response.text.strip()
            # Parse the simple response format: "5\t1\t3"
            parts = result_text.split('\t')
            if len(parts) >= 3:
                record_count = int(parts[0])
                unique_batches = int(parts[1])
                unique_clients = int(parts[2])
                
                print(f"📊 Query Results:")
                print(f"   Total Records: {record_count}")
                print(f"   Unique Batches: {unique_batches}")
                print(f"   Unique Clients: {unique_clients}")
                
                if record_count >= 5:
                    print(f"✅ PASS: All test records verified in ClickHouse")
                    return True, record_count
                else:
                    print(f"⚠️  PARTIAL: Found {record_count} records (expected 5+)")
                    return False, record_count
        else:
            print(f"❌ FAIL: Query returned status {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False, 0
            
    except Exception as ex:
        print(f"❌ FAIL: Could not verify data: {type(ex).__name__}: {ex}")
        return False, 0

def test_phase_5_checkpoint():
    print_section("PHASE 5: Test Checkpoint Functionality")
    
    print("[TEST] Loading checkpoint from ClickHouse...")
    
    checkpoint = load_idp_accuracy_checkpoint(timeout=10)
    
    if checkpoint:
        print(f"✅ PASS: Checkpoint loaded successfully")
        print(f"   Last checkpoint: {checkpoint}")
        return True
    else:
        print(f"⚠️  INFO: No checkpoint found (may be expected if table is new)")
        return True  # Don't fail on this, it's expected initially

def main():
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "  CLICKHOUSE HTTP INTERFACE - COMPREHENSIVE TEST SUITE  ".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "="*78 + "╝")
    
    test_results = []
    
    # Phase 1: Connection
    result1 = test_phase_1_connection()
    test_results.append(("Phase 1: Connection Test", result1))
    if not result1:
        print("\n⚠️  Stopping tests - cannot connect to ClickHouse")
        print_results(test_results)
        return 1
    
    # Phase 2: Database Init
    result2 = test_phase_2_database_init()
    test_results.append(("Phase 2: Database Initialization", result2))
    if not result2:
        print("\n⚠️  Stopping tests - cannot initialize database")
        print_results(test_results)
        return 1
    
    # Phase 3: Insert Data
    result3, insert_count = test_phase_3_insert_sample_data()
    test_results.append(("Phase 3: Data Insertion", result3))
    
    # Wait a moment for data to be committed
    if result3:
        print("\n[INFO] Waiting 2 seconds for data to be committed...")
        time.sleep(2)
    
    # Phase 4: Verify Data
    result4, verify_count = test_phase_4_verify_data()
    test_results.append(("Phase 4: Data Verification", result4))
    
    # Phase 5: Checkpoint
    result5 = test_phase_5_checkpoint()
    test_results.append(("Phase 5: Checkpoint Loading", result5))
    
    # Summary
    print_results(test_results)
    
    all_passed = all(result for _, result in test_results if isinstance(result, bool))
    
    if all_passed:
        print_section("🎉 ALL TESTS PASSED!")
        print("✅ ClickHouse HTTP interface is working correctly")
        print("✅ Data can be inserted and verified")
        print("✅ RCA fix implementation is ready for production")
        return 0
    else:
        print_section("⚠️  SOME TESTS FAILED")
        print("Review errors above and check:")
        print("1. ClickHouse HTTP service is running on port 8123")
        print("2. Network connectivity to 172.173.148.33")
        print("3. ClickHouse credentials in local.settings.json")
        print("4. RCA logs in AUX_code/data_push.log")
        return 1

def print_results(test_results):
    print_section("TEST RESULTS SUMMARY")
    
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}  -  {test_name}")
    
    passed = sum(1 for _, r in test_results if r)
    total = len(test_results)
    
    print(f"\n{passed}/{total} tests passed")
    
    print("\nRCA Log File: AUX_code/data_push.log")
    print("View logs with: Get-Content ./AUX_code/data_push.log -Tail 50")

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⏹️  Test interrupted by user")
        sys.exit(1)
    except Exception as ex:
        print(f"\n\n❌ FATAL ERROR: {type(ex).__name__}: {ex}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
