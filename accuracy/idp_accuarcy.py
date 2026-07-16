import argparse
import csv
import os
import time
import logging
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyodbc

# Setup RCA logging
rca_log_dir = Path(__file__).resolve().parents[1].parent / "AUX_code"
rca_log_dir.mkdir(parents=True, exist_ok=True)
rca_log_file = rca_log_dir / "data_push.log"

rca_logger = logging.getLogger("IDP_ACCURACY_RCA")
rca_logger.setLevel(logging.DEBUG)

# File handler
fh = logging.FileHandler(rca_log_file, mode='a', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] (%(name)s:%(lineno)d) - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
rca_logger.addHandler(fh)

# Import HTTP-based ClickHouse connectivity (port 8123 - confirmed working)
from clickhouse_store import (
    get_environment, 
    load_idp_accuracy_checkpoint,
    insert_idp_transactions_http,
    ensure_database_and_table,
    test_clickhouse_connection,
    logger as ch_logger
)

# Wrapper for compatibility with existing code
def insert_idp_transactions(
    environment: str,
    records: List[Dict[str, Any]],
    checkpoint_datetime: datetime
) -> None:
    """Insert IDP accuracy records to ClickHouse via HTTP interface."""
    print(f"[ClickHouse] insert_idp_transactions() called with {len(records)} records")
    result = insert_idp_transactions_http(
        environment=environment,
        records=records,
        checkpoint_datetime=checkpoint_datetime,
        timeout=60
    )
    if not result:
        raise Exception("Failed to insert records to ClickHouse")


try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:
    find_dotenv = None
    load_dotenv = None


def load_env() -> None:
    if load_dotenv and find_dotenv:
        env_path = find_dotenv(usecwd=True)
        if env_path:
            load_dotenv(env_path, override=False)
        else:
            load_dotenv(override=False)
        return

    env_file = Path(".env")
    if not env_file.exists():
        return

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in ("'", '"')
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_env()


def _load_local_settings_if_available() -> None:
    """Load settings from local.settings.json at module level."""
    root = Path(__file__).resolve().parents[1]
    settings_path = root / "local.settings.json"
    if not settings_path.exists():
        return

    try:
        import json as json_module
        with settings_path.open("r", encoding="utf-8") as f:
            values = json_module.load(f).get("Values", {})
        count = 0
        for key, value in values.items():
            if key not in os.environ and value is not None:
                os.environ[key] = str(value)
                count += 1
        print(f"[INFO] Loaded {count} environment variables from local.settings.json")
    except Exception as ex:
        print(f"[WARN] Could not load local.settings.json: {ex}")


_load_local_settings_if_available()


WRITE_CSV = False
UPLOAD_TO_CLICKHOUSE = True
CHUNK_SIZE = 1000  

CLICKHOUSE_ENVIRONMENT = get_environment()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch data from dbo.vw_PdfClassificationTransactionLog."
    )
    parser.add_argument("--start-date", help="Optional filter, format: YYYY-MM-DD")
    parser.add_argument("--end-date", help="Optional filter, format: YYYY-MM-DD")
    parser.add_argument(
        "--output",
        default="healthcare/pdf_classification_transaction_log.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging during execution.",
    )
    args, _ = parser.parse_known_args()
    return args


def validate_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    datetime.strptime(value, "%Y-%m-%d")
    return value


def build_query(
    start_date: Optional[str],
    end_date: Optional[str],
    include_order: bool = True,
    include_all_columns: bool = True,
    verbose: bool = False,
) -> tuple[str, List[str]]:
    base_columns = "v.*" if include_all_columns else "v.Id, v.CreatedOn, v.BatchId"
    outer_columns = "p.*" if include_all_columns else "p.Id, p.CreatedOn, p.BatchId, p.Filename, p.PredictedCategory, p.ClientCode"
    query_lines = [
        "WITH parsed AS (",
        "    SELECT",
        f"        {base_columns},",
        "        CASE",
        "            WHEN ISJSON(v.ResponsePayload) = 1 THEN COALESCE(",
        "                JSON_VALUE(v.ResponsePayload, '$.json[0].\"File Name\"'),",
        "                JSON_VALUE(v.ResponsePayload, '$.\"File Name\"')",
        "            )",
        "            ELSE NULL",
        "        END AS Filename,",
        "        CASE",
        "            WHEN ISJSON(v.ResponsePayload) = 1 THEN COALESCE(",
        "                JSON_VALUE(v.ResponsePayload, '$.json[0].\"Predicted Category\"'),",
        "                JSON_VALUE(v.ResponsePayload, '$.\"Predicted Category\"')",
        "            )",
        "            ELSE NULL",
        "        END AS PredictedCategory,",
        "        CASE",
        "            WHEN ISJSON(v.ResponsePayload) = 1 THEN COALESCE(",
        "                JSON_VALUE(v.ResponsePayload, '$.json[0].\"Client Code\"'),",
        "                JSON_VALUE(v.ResponsePayload, '$.\"Client Code\"')",
        "            )",
        "            ELSE NULL",
        "        END AS ClientCode",
        "    FROM dbo.vw_PdfClassificationTransactionLog v",
    ]
    params: List[str] = []
    where_clauses: List[str] = []

    if start_date:
        where_clauses.append("CreatedOn >= ?")
        params.append(start_date)
    if end_date:
        where_clauses.append("CreatedOn < DATEADD(DAY, 1, ?)")
        params.append(end_date)

    if where_clauses:
        query_lines.append("    WHERE " + " AND ".join(where_clauses))

    query_lines.extend(
        [
            ")",
            "SELECT",
            f"    {outer_columns},",
            "    CASE",
            "        WHEN p.PredictedCategory IS NULL THEN 0",
            "        WHEN LOWER(LTRIM(RTRIM(p.PredictedCategory))) IN ('', 'other', 'others', 'unknown', '/unknown/') THEN 0",
            "        ELSE 1",
            "    END AS IsAccurate",
            "FROM parsed p",
        ]
    )

    if include_order:
        query_lines.append("ORDER BY p.CreatedOn ASC")
    query_lines.append(";")
    
    query = "\n".join(query_lines)
    
    if verbose:
        print("\n[Verbose] Generated SQL Query:")
        print(query)
        print(f"[Verbose] Query Parameters: {params}\n")
        
    return query, params


def fetch_rows(
    server: str,
    database: str,
    user: str,
    password: str,
    start_date: Optional[str],
    end_date: Optional[str],
    include_order: bool = True,
    include_all_columns: bool = True,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    print(f"[TIMING] fetch_rows() started at {datetime.now()}")
    overall_start = time.perf_counter()
    
    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
    installed_drivers = set(pyodbc.drivers())
    selected_driver = next((d for d in preferred_drivers if d in installed_drivers), None)
    
    if verbose:
        print(f"[Verbose] Installed ODBC Drivers: {list(installed_drivers)}")
        print(f"[Verbose] Selected ODBC Driver: {selected_driver}")
        
    if not selected_driver:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Install ODBC Driver 18 or 17 for SQL Server."
        )

    conn_str = (
        f"DRIVER={{{selected_driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Connect Timeout=30;"
    )
    if "ODBC Driver" in selected_driver:
        conn_str += "Encrypt=yes;TrustServerCertificate=no;"

    if verbose:
        safe_conn_str = (
            f"DRIVER={{{selected_driver}}};SERVER={server};DATABASE={database};"
            f"UID={user};PWD=********;Connect Timeout=30;"
        )
        print(f"[Verbose] Connection String: {safe_conn_str}")

    print(f"[TIMING]   Connecting to SQL Server...")
    conn_start = time.perf_counter()
    with pyodbc.connect(conn_str) as conn:
        conn_duration = time.perf_counter() - conn_start
        print(f"[TIMING]   Connection established in {conn_duration:.3f} seconds")
        
        cursor = conn.cursor()
        query, params = build_query(
            start_date=start_date,
            end_date=end_date,
            include_order=include_order,
            include_all_columns=include_all_columns,
            verbose=verbose,
        )
        
        if verbose:
            print("[Verbose] Executing query on SQL Server...")
            
        print(f"[TIMING]   Executing SQL query...")
        query_start = time.perf_counter()
        cursor.execute(query, params)
        query_duration = time.perf_counter() - query_start
        print(f"[TIMING]   Query executed in {query_duration:.3f} seconds")
        
        print(f"[TIMING]   Fetching results...")
        fetch_start = time.perf_counter()
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        fetch_duration = time.perf_counter() - fetch_start
        print(f"[TIMING]   Results fetched in {fetch_duration:.3f} seconds ({len(rows)} rows)")
        
        print(f"[TIMING]   Converting to dictionaries...")
        convert_start = time.perf_counter()
        result = [dict(zip(columns, row)) for row in rows]
        convert_duration = time.perf_counter() - convert_start
        print(f"[TIMING]   Conversion completed in {convert_duration:.3f} seconds")
        
        overall_duration = time.perf_counter() - overall_start
        print(f"[TIMING] fetch_rows() completed in {overall_duration:.3f} seconds total")
        
        if verbose:
            print(f"[Verbose] Query completed in {query_duration:.3f} seconds.")
            print(f"[Verbose] Fetched {len(result)} raw rows from database.")
            
        return result


def write_csv(output_path: Path, rows: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _load_checkpoint_from_clickhouse(verbose: bool = False) -> Optional[datetime]:
    """Skip ClickHouse checkpoint loading due to connectivity issues."""
    print(f"[ClickHouse] Skipping checkpoint loading (ClickHouse unavailable)")
    return None


def process_and_upload_date_wise(rows: List[Dict[str, Any]], verbose: bool = False) -> None:
    if not rows:
        print("[ClickHouse] No rows retrieved to process.")
        rca_logger.info("[ClickHouse] No rows to process")
        return

    if verbose:
        print(f"[Verbose] Starting date-wise processing of {len(rows)} rows...")
        rca_logger.debug(f"[RCA] Date-wise processing started for {len(rows)} rows")

    rows_by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        created_on = row.get("CreatedOn")
        if isinstance(created_on, datetime):
            date_str = created_on.strftime("%Y-%m-%d")
        elif isinstance(created_on, date):
            date_str = created_on.strftime("%Y-%m-%d")
        elif isinstance(created_on, str) and len(created_on) >= 10:
            date_str = created_on[:10]
        else:
            date_str = "UNKNOWN"
        rows_by_date[date_str].append(row)

    sorted_dates = sorted([d for d in rows_by_date if d != "UNKNOWN"])
    if "UNKNOWN" in rows_by_date:
        sorted_dates.append("UNKNOWN")

    print(f"\n[Processing] Found {len(rows)} records spanning {len(sorted_dates)} distinct dates.")
    rca_logger.info(f"[RCA] Processing {len(rows)} records across {len(sorted_dates)} dates: {sorted_dates}")
    
    if verbose:
        print(f"[Verbose] Chronological date processing sequence: {sorted_dates}")
        rca_logger.debug(f"[RCA] Date sequence: {sorted_dates}")

    successful_uploads = 0
    failed_uploads = 0
    
    for current_date in sorted_dates:
        date_rows = rows_by_date[current_date]
        total_date_rows = len(date_rows)
        print(f"\n--- Processing Date: {current_date} ({total_date_rows} total records) ---")
        rca_logger.info(f"[RCA] Processing date={current_date}, records={total_date_rows}")

        for chunk_idx in range(0, total_date_rows, CHUNK_SIZE):
            chunk = date_rows[chunk_idx : chunk_idx + CHUNK_SIZE]
            batch_records = []
            last_created_on_in_chunk: Optional[datetime] = None

            for row in chunk:
                row_created_on = row.get("CreatedOn")
                row_dt: Optional[datetime] = None
                if isinstance(row_created_on, datetime):
                    row_dt = row_created_on
                elif isinstance(row_created_on, str):
                    try:
                        row_dt = datetime.fromisoformat(row_created_on.replace("Z", ""))
                    except ValueError:
                        pass
                
                if row_dt:
                    if not last_created_on_in_chunk or row_dt > last_created_on_in_chunk:
                        last_created_on_in_chunk = row_dt

                record = {
                    "BatchId": row.get("BatchId"),
                    "Filename": row.get("Filename") or "UNKNOWN",
                    "ClientCode": row.get("ClientCode") or "UNKNOWN",
                    "PredictedCategory": row.get("PredictedCategory") or "UNKNOWN",
                    "CreatedOn": row.get("CreatedOn")
                }
                batch_records.append(record)

                if verbose and len(batch_records) <= 2:
                    print(f"[Verbose Sample] Record #{len(batch_records)}: {record}")
                    rca_logger.debug(f"[RCA] Sample record: {record}")

            if verbose and len(batch_records) > 2:
                print(f"[Verbose] ...and {len(batch_records) - 2} other records in this chunk.")

            if UPLOAD_TO_CLICKHOUSE:
                try:
                    print("[ClickHouse] Preparing to upload chunk...")
                    checkpoint_to_save = last_created_on_in_chunk if last_created_on_in_chunk else datetime.utcnow()
                    
                    print(
                        f"[ClickHouse] Uploading chunk of {len(batch_records)} records "
                        f"(indices {chunk_idx} to {chunk_idx + len(batch_records) - 1}) for {current_date}..."
                    )
                    rca_logger.info(
                        f"[RCA] Chunk upload started: date={current_date}, "
                        f"chunk_idx={chunk_idx}, records={len(batch_records)}, "
                        f"checkpoint={checkpoint_to_save}"
                    )
                    
                    insert_idp_transactions(
                        environment=CLICKHOUSE_ENVIRONMENT,
                        records=batch_records,
                        checkpoint_datetime=checkpoint_to_save
                    )
                    
                    print(f"[ClickHouse] Successfully uploaded chunk. Checkpoint updated to: {checkpoint_to_save}")
                    rca_logger.info(
                        f"[RCA] Chunk upload successful: date={current_date}, "
                        f"records_uploaded={len(batch_records)}"
                    )
                    successful_uploads += 1
                    
                except Exception as e:
                    print(f"[ClickHouse ERROR] Failed to upload chunk at index {chunk_idx} for date {current_date}: {e}")
                    rca_logger.error(
                        f"[RCA] Chunk upload FAILED: date={current_date}, "
                        f"chunk_idx={chunk_idx}, records={len(batch_records)}, "
                        f"error={type(e).__name__}: {str(e)[:200]}"
                    )
                    failed_uploads += 1
                    print(f"[WARNING] Continuing with next chunk despite upload failure...")
                    # Continue to next chunk instead of raising
                    continue
    
    rca_logger.info(
        f"[RCA] Processing complete: successful={successful_uploads}, failed={failed_uploads}"
    )


def main() -> int:
    print("\n" + "=" * 80)
    print(f"[TIMING] Script execution started at {datetime.now()}")
    script_start = time.perf_counter()
    print("=" * 80 + "\n")
    
    rca_logger.info("="*80)
    rca_logger.info("[RCA] ===== IDP ACCURACY PIPELINE EXECUTION START =====")
    rca_logger.info("="*80)
    
    args = parse_args()
    
    if args.verbose:
        print("[Verbose] Verbose logging is active.")
        print(f"[Verbose] Active ClickHouse Environment: {CLICKHOUSE_ENVIRONMENT}")
        rca_logger.debug(f"[RCA] Environment: {CLICKHOUSE_ENVIRONMENT}")
        
    server = os.getenv("IDP_SQL_SERVER")
    database = os.getenv("IDP_SQL_DATABASE")
    user = os.getenv("IDP_SQL_USER")
    password = os.getenv("IDP_SQL_PASSWORD", "")

    rca_logger.info(f"[RCA] SQL Config: server={server}, database={database}, user={user}")

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    
    if not start_date:
        print("[Config] Loading checkpoint from ClickHouse")
        rca_logger.info("[RCA] Phase 1: Loading ClickHouse checkpoint")
        
        checkpoint = _load_checkpoint_from_clickhouse(verbose=args.verbose)
        
        if checkpoint:
            start_date = checkpoint.strftime("%Y-%m-%dT%H:%M:%S")
            print(f"[Checkpoint] Using start_datetime for incremental fetch: {start_date}")
            print(f"[Info] Fetching only data AFTER: {start_date}")
            rca_logger.info(f"[RCA] Checkpoint loaded: {checkpoint}, will fetch data after this timestamp")
        else:
            print("[Checkpoint] No prior checkpoint found. Fetching from start.")
            rca_logger.info("[RCA] No checkpoint found, fetching all data from start")
    elif args.verbose:
        print(f"[Verbose] Explicit start date provided via argument: {start_date}")
        rca_logger.debug(f"[RCA] Explicit start_date: {start_date}")
        
    if not server or not database or not user or not password:
        rca_logger.error("[RCA] FAILED: Missing SQL configuration")
        raise ValueError(
            "Missing SQL config. Set IDP_SQL_SERVER, IDP_SQL_DATABASE, "
            "IDP_SQL_USER, and IDP_SQL_PASSWORD in .env."
        )

    print("\n" + "=" * 80)
    print("[PHASE] Starting SQL Data Fetch")
    print("=" * 80)
    rca_logger.info("[RCA] Phase 2: SQL Data Fetch (connecting to Azure SQL Server)")
    fetch_start = time.perf_counter()
    
    rows = fetch_rows(
        server=server,
        database=database,
        user=user,
        password=password,
        start_date=start_date,
        end_date=end_date,
        include_order=True,
        include_all_columns=WRITE_CSV,
        verbose=args.verbose,
    )
    fetch_duration = time.perf_counter() - fetch_start
    print(f"\n[TIMING] SQL fetch phase completed in {fetch_duration:.3f} seconds")
    print(f"[TIMING] Retrieved {len(rows)} total rows\n")
    rca_logger.info(f"[RCA] SQL fetch complete: {len(rows)} rows fetched in {fetch_duration:.3f}s")

    output_path = Path(args.output)
    if WRITE_CSV:
        if args.verbose:
            print(f"[Verbose] Writing raw transaction records to CSV: {output_path}")
            rca_logger.debug(f"[RCA] CSV output: {output_path}")
        write_csv(output_path, rows)

    total_rows = len(rows)
    accurate_rows = 0
    if total_rows:
        accurate_rows = sum(1 for row in rows if int(row.get("IsAccurate") or 0) == 1)
    accuracy_pct = (accurate_rows / total_rows * 100.0) if total_rows else 0.0

    per_client: Dict[str, Dict[str, int]] = {}
    for row in rows:
        client_code_raw = row.get("ClientCode")
        client_code = str(client_code_raw).strip() if client_code_raw is not None else ""
        if not client_code:
            client_code = "UNKNOWN"
        stats = per_client.setdefault(client_code, {"total": 0, "accurate": 0})
        stats["total"] += 1
        if int(row.get("IsAccurate") or 0) == 1:
            stats["accurate"] += 1

    print(f"Rows fetched: {len(rows)}")
    print(f"Accurate rows (Valid Predicted Category): {accurate_rows}")
    print(f"Inaccurate rows (Other/Others/Unknown/Blank): {total_rows - accurate_rows}")
    print(f"Accuracy: {accurate_rows}/{total_rows} ({accuracy_pct:.2f}%)")
    
    rca_logger.info(f"[RCA] Accuracy Calculation: total={total_rows}, accurate={accurate_rows}, pct={accuracy_pct:.2f}%")
    
    if per_client:
        print("\nClient-wise Accuracy:")
        print("Client Code | Accurate | Total | Accuracy%")
        client_summary = []
        for client_code in sorted(per_client):
            client_total = per_client[client_code]["total"]
            client_accurate = per_client[client_code]["accurate"]
            client_accuracy = (client_accurate / client_total * 100.0) if client_total else 0.0
            print(f"{client_code} | {client_accurate} | {client_total} | {client_accuracy:.2f}%")
            client_summary.append(f"{client_code}:{client_accurate}/{client_total}({client_accuracy:.2f}%)")
        
        rca_logger.info(f"[RCA] Client breakdown: {', '.join(client_summary)}")
    
    if WRITE_CSV:
        print(f"CSV written to: {output_path.resolve()}")
    else:
        if args.verbose:
            print("[Verbose] CSV writing disabled (WRITE_CSV=False).")
    
    # Process and upload data date-wise to ClickHouse
    print(f"\n" + "=" * 80)
    print("[PHASE] Starting ClickHouse Upload")
    print("=" * 80)
    rca_logger.info("[RCA] Phase 3: ClickHouse Data Upload (HTTP interface port 8123)")
    upload_start = time.perf_counter()

    # Ensure database and table exist before uploading
    print("[ClickHouse] Ensuring database and table exist...")
    if not ensure_database_and_table():
        rca_logger.error("[RCA] Failed to create/verify ClickHouse table. Aborting upload.")
        raise RuntimeError("ClickHouse table initialization failed.")
    print("[ClickHouse] Database and table verified.")

    if total_rows > 0:
        try:
            process_and_upload_date_wise(rows, verbose=args.verbose)
            upload_duration = time.perf_counter() - upload_start
            print(f"\n[TIMING] ClickHouse upload phase completed in {upload_duration:.3f} seconds")
            rca_logger.info(f"[RCA] ClickHouse upload complete: {upload_duration:.3f}s")
        except Exception as upload_ex:
            upload_duration = time.perf_counter() - upload_start
            print(f"\n[ClickHouse ERROR] Upload failed after {upload_duration:.3f} seconds: {upload_ex}")
            rca_logger.error(f"[RCA] ClickHouse upload FAILED: {type(upload_ex).__name__}: {upload_ex}")
            print(f"[WARNING] Data was not uploaded to ClickHouse, but SQL data was successfully processed")
    else:
        if UPLOAD_TO_CLICKHOUSE:
            print("[ClickHouse] No new rows to upload.")
            rca_logger.info("[RCA] No rows to upload (total_rows=0)")
    
    # Final timing summary
    total_duration = time.perf_counter() - script_start
    print("\n" + "=" * 80)
    print("[TIMING SUMMARY]")
    print("=" * 80)
    print(f"Total script execution time: {total_duration:.3f} seconds")
    if 'fetch_duration' in locals():
        print(f"  - SQL fetch phase: {fetch_duration:.3f} seconds")
    print("=" * 80 + "\n")
    
    rca_logger.info("[RCA] ===== IDP ACCURACY PIPELINE EXECUTION COMPLETE =====")
    rca_logger.info(f"[RCA] Total duration: {total_duration:.3f}s")
    rca_logger.info("="*80)
    
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        raise SystemExit(130)