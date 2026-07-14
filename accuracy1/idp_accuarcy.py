import argparse
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyodbc

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


# Load environment variables first
load_env()


WRITE_CSV = False
UPLOAD_TO_LANGFUSE = True

# Langfuse configuration
LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT").strip()



IDP_SUMMARY_DATASET_NAME = f"idp_accuracy_summary_{LANGFUSE_ENVIRONMENT}"
IDP_CLIENT_DATASET_NAME = f"idp_accuracy_client_{LANGFUSE_ENVIRONMENT}"


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
    # Azure Functions injects worker-specific CLI args; ignore unknown options.
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
) -> tuple[str, List[str]]:
    base_columns = "v.*" if include_all_columns else "v.Id, v.CreatedOn"
    outer_columns = "p.*" if include_all_columns else "p.Id, p.CreatedOn, p.PredictedCategory, p.ClientCode"
    query_lines = [
        "WITH parsed AS (",
        "    SELECT",
        f"        {base_columns},",
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

    # The accuracy case block now considers blanks, NULLs, 'other', 'others', 
    # 'unknown', and '/unknown/' as inaccurate (0), and everything else as accurate (1).
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
        query_lines.append("ORDER BY p.CreatedOn DESC")
    query_lines.append(";")
    return "\n".join(query_lines), params


def fetch_rows(
    server: str,
    database: str,
    user: str,
    password: str,
    start_date: Optional[str],
    end_date: Optional[str],
    include_order: bool = True,
    include_all_columns: bool = True,
) -> List[Dict[str, Any]]:
    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
    installed_drivers = set(pyodbc.drivers())
    selected_driver = next((d for d in preferred_drivers if d in installed_drivers), None)
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

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        query, params = build_query(
            start_date=start_date,
            end_date=end_date,
            include_order=include_order,
            include_all_columns=include_all_columns,
        )
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def write_csv(output_path: Path, rows: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _get_langfuse_client():
    """Get Langfuse client for uploading datasets."""
    try:
        from langfuse import Langfuse

        public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
        secret_key = os.getenv("LANGFUSE_SECRET_KEY")
        host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
        
        print(f"[Langfuse] Initializing client...")
        print(f"[Langfuse] Host: {host}")
        print(f"[Langfuse] Public Key set: {bool(public_key)}")
        print(f"[Langfuse] Secret Key set: {bool(secret_key)}")
        
        if not public_key or not secret_key:
            print("[Langfuse] Missing LANGFUSE_PUBLIC_KEY or LANGFUSE_SECRET_KEY in .env")
            return None

        client = Langfuse(
            public_key=public_key.strip(), 
            secret_key=secret_key.strip(), 
            host=host.strip(),
        )
        print("[Langfuse] Client initialized successfully")
        return client
    except ImportError:
        print("[Langfuse] langfuse package not installed. Install with: pip install langfuse")
        return None
    except Exception as e:
        print(f"[Langfuse] Error initializing client: {e}")
        import traceback
        traceback.print_exc()
        return None


def _load_checkpoint_from_langfuse() -> Optional[datetime]:
    """Load implicit checkpoint from the latest summary dataset item in Langfuse."""
    langfuse = _get_langfuse_client()
    if not langfuse:
        return None

    try:
        dataset = langfuse.get_dataset(IDP_SUMMARY_DATASET_NAME)
        if not dataset:
            return None

        if hasattr(dataset, 'items') and dataset.items:
            latest_item = max(dataset.items, key=lambda r: getattr(r, 'created_at', datetime.min))
            checkpoint_data = getattr(latest_item, 'input', None)
            if isinstance(checkpoint_data, dict):
                candidate = checkpoint_data.get("datetime") or checkpoint_data.get("timestamp")
                if candidate:
                    try:
                        return datetime.fromisoformat(str(candidate))
                    except (ValueError, TypeError):
                        pass

            created_at = getattr(latest_item, 'created_at', None)
            if isinstance(created_at, datetime):
                return created_at
        return None
    except Exception as ex:
        print(f"[Langfuse] Error loading checkpoint: {ex}")
        return None

def _upload_to_langfuse(
    accuracy_pct: float,
    total_rows: int,
    accurate_rows: int,
    inaccurate_rows: int,
    per_client: Dict[str, Dict[str, int]],
    run_datetime: datetime,
) -> None:
    """Upload accuracy results to Langfuse datasets."""
    langfuse = _get_langfuse_client()
    if not langfuse:
        print("[Langfuse] Client not initialized. Skipping upload.")
        return

    try:
        print(f"[Langfuse] Starting upload to datasets...")
        print(f"[Langfuse] Summary dataset: {IDP_SUMMARY_DATASET_NAME}")
        print(f"[Langfuse] Client dataset: {IDP_CLIENT_DATASET_NAME}")
        
        # Create summary dataset
        try:
            langfuse.create_dataset(
                name=IDP_SUMMARY_DATASET_NAME,
                description=f"IDP accuracy summary ({LANGFUSE_ENVIRONMENT})"
            )
            print(f"[Langfuse] Created summary dataset (or already exists)")
        except Exception as e:
            print(f"[Langfuse] Dataset creation note: {e}")

        # Create client dataset
        try:
            langfuse.create_dataset(
                name=IDP_CLIENT_DATASET_NAME,
                description=f"IDP accuracy client breakdown ({LANGFUSE_ENVIRONMENT})"
            )
            print(f"[Langfuse] Created client dataset (or already exists)")
        except Exception as e:
            print(f"[Langfuse] Dataset creation note: {e}")

        # Upload overall summary
        summary_item_id = f"summary::{run_datetime.isoformat()}"
        summary_payload = {
            "datetime": run_datetime.isoformat(),
            "accurate_rows": int(accurate_rows),
            "inaccurate_rows": int(inaccurate_rows),
            "total_rows": int(total_rows),
            "accuracy_pct": round(accuracy_pct, 2),
        }
        
        try:
            langfuse.create_dataset_item(
                dataset_name=IDP_SUMMARY_DATASET_NAME,
                id=summary_item_id,
                input=summary_payload,
                metadata={
                    "record_type": "idp_accuracy_summary",
                    "timestamp": run_datetime.isoformat(),
                }
            )
            print(f"[Langfuse] Summary item created: {summary_item_id}")
        except Exception as e:
            print(f"[Langfuse] Error pushing summary data: {e}")
            import traceback
            traceback.print_exc()

        # Upload per-client accuracy
        for client_code in sorted(per_client):
            client_total = per_client[client_code]["total"]
            client_accurate = per_client[client_code]["accurate"]
            client_inaccurate = client_total - client_accurate
            client_accuracy = (client_accurate / client_total * 100.0) if client_total else 0.0

            client_item_id = f"client::{client_code}::{run_datetime.isoformat()}"
            client_payload = {
                "datetime": run_datetime.isoformat(),
                "client_code": client_code,
                "accurate": int(client_accurate),
                "inaccurate": int(client_inaccurate),
                "total": int(client_total),
                "accuracy_pct": round(client_accuracy, 2),
            }
            
            try:
                langfuse.create_dataset_item(
                    dataset_name=IDP_CLIENT_DATASET_NAME,
                    id=client_item_id,
                    input=client_payload,
                    metadata={
                        "record_type": "idp_accuracy_client",
                        "client_code": client_code,
                        "timestamp": run_datetime.isoformat(),
                    }
                )
                print(f"[Langfuse] Client item created: {client_item_id}")
            except Exception as e:
                print(f"[Langfuse] Error pushing client data for '{client_code}': {e}")

        print(f"[Langfuse] Uploaded {len(per_client)} client records to dataset '{IDP_CLIENT_DATASET_NAME}'")
        
        # Flush to ensure data is sent
        try:
            langfuse.flush()
            print(f"[Langfuse] Data flushed successfully")
        except Exception as e:
            print(f"[Langfuse] Error flushing data: {e}")

    except Exception as ex:
        print(f"[Langfuse] Error uploading results: {ex}")
        import traceback
        traceback.print_exc()



def main() -> int:
    args = parse_args()
    server = os.getenv("IDP_SQL_SERVER")
    database = os.getenv("IDP_SQL_DATABASE")
    user = os.getenv("IDP_SQL_USER")
    password = os.getenv("IDP_SQL_PASSWORD", "")

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    
    # Load checkpoint if no explicit start-date is provided.
    # Checkpoint is inferred from latest saved summary dataset row.
    if not start_date:
        print("[Config] Loading checkpoint from latest Langfuse summary dataset row")
        checkpoint = _load_checkpoint_from_langfuse()
        
        if checkpoint:
            # Use full datetime (not just date) to avoid refetching same day's data
            start_date = checkpoint.strftime("%Y-%m-%dT%H:%M:%S")
            print(f"[Checkpoint] Using start_datetime for incremental fetch: {start_date}")
            print(f"[Info] This will fetch only data AFTER: {start_date}")
        else:
            print("[Checkpoint] No prior summary row found in Langfuse. Fetching all data from beginning.")
    
    if not server or not database or not user or not password:
        raise ValueError(
            "Missing SQL config. Set IDP_SQL_SERVER, IDP_SQL_DATABASE, IDP_SQL_USER, and IDP_SQL_PASSWORD "
            "in .env."
        )

    rows = fetch_rows(
        server=server,
        database=database,
        user=user,
        password=password,
        start_date=start_date,
        end_date=end_date,
        include_order=WRITE_CSV,
        include_all_columns=WRITE_CSV,
    )

    output_path = Path(args.output)
    if WRITE_CSV:
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
    if per_client:
        print("\nClient-wise Accuracy:")
        print("Client Code | Accurate | Total | Accuracy%")
        for client_code in sorted(per_client):
            client_total = per_client[client_code]["total"]
            client_accurate = per_client[client_code]["accurate"]
            client_accuracy = (client_accurate / client_total * 100.0) if client_total else 0.0

            print(f"{client_code} | {client_accurate} | {client_total} | {client_accuracy:.2f}%")
    
    if WRITE_CSV:
        print(f"CSV written to: {output_path.resolve()}")
    else:
        print("CSV writing skipped (WRITE_CSV=False).")
    
    # Save to local datasets if enabled and rows were fetched
    if total_rows > 0:
        run_datetime = datetime.utcnow()

        # Upload to Langfuse if enabled
        if UPLOAD_TO_LANGFUSE:
            _upload_to_langfuse(
                accuracy_pct=accuracy_pct,
                total_rows=total_rows,
                accurate_rows=accurate_rows,
                inaccurate_rows=total_rows - accurate_rows,
                per_client=per_client,
                run_datetime=run_datetime,
            )
            print("[Langfuse] Upload completed successfully.")

        # Checkpoint is persisted to Langfuse inside _upload_to_langfuse.
    else:
        if UPLOAD_TO_LANGFUSE:
            print("[Langfuse] No new rows to upload.")
    
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        raise SystemExit(130)