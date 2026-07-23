"""
Drop all tables in the ClickHouse accuracy_and_finetuning database.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import requests
from clickhouse_store import _get_clickhouse_config

def main():
    host, http_port, database, user, password = _get_clickhouse_config()
    base_url = f"http://{host}:{http_port}/"

    # 1. List all tables in the database
    list_query = f"SELECT name FROM system.tables WHERE database = '{database}' FORMAT TSV"
    resp = requests.get(base_url, auth=(user, password), params={"query": list_query}, timeout=15)
    resp.raise_for_status()

    tables = [line.strip() for line in resp.text.splitlines() if line.strip()]
    if not tables:
        print(f"[ClickHouse] No tables found in '{database}'. Nothing to drop.")
        return

    print(f"[ClickHouse] Found {len(tables)} table(s) in '{database}':")
    for t in tables:
        print(f"  - {t}")

    # 2. Drop each table
    for table in tables:
        ddl = f"DROP TABLE IF EXISTS `{database}`.`{table}`"
        drop_resp = requests.post(base_url, auth=(user, password), data=ddl.encode(), timeout=30)
        if drop_resp.status_code in (200, 201):
            print(f"  [DROPPED] {table}")
        else:
            print(f"  [FAILED]  {table} -> {drop_resp.status_code}: {drop_resp.text[:200]}")

    print(f"\n[ClickHouse] Done. All tables dropped from '{database}'.")

if __name__ == "__main__":
    main()
