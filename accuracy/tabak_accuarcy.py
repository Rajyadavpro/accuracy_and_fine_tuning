

# """
# MariaDB Direct Audit Accuracy Evaluator
# ---------------------------------------
# This script connects directly to a MariaDB database, extracts target audit fields 
# utilizing database-level JSON capabilities, and calculates the overall and class-wise 
# (TP, FP, FN) classification metrics over the raw eligible transactions.

# It supports time-period filtering (--period), persists raw records to ClickHouse,
# and infers incremental checkpoints from the latest saved file_receive_date.
# """

# import argparse
# import os
# import re
# from datetime import datetime
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple

# from dotenv import load_dotenv

# from clickhouse_store import (
#     get_environment,
#     insert_tabak_raw_data,
#     load_tabak_accuracy_checkpoint,
#     ensure_tabak_raw_table,
# )

# # Load database credentials and config from the environment (.env file)
# ENV_PATH = Path(__file__).resolve().with_name(".env")
# load_dotenv(dotenv_path=ENV_PATH)


# UPLOAD_TO_CLICKHOUSE = True

# ENVIRONMENT = get_environment()
# CATEGORIES = ["VA_Rating_Decision", "VA_Fee_Letter", "Others"]


# # ==========================================
# # Database Connection & Query Functions
# # ==========================================

# def get_db_credentials():
#     """
#     Retrieves and parses database connection variables.
#     Handles standard JDBC URLs or explicit environment variable fallbacks.
#     """
#     jdbc_url = os.getenv("TABAK_DB_JDBC_URL", "").strip()
#     server, port, database = "", "3306", ""

#     # Try parsing server and database out of a JDBC URL string if present
#     if jdbc_url:
#         match = re.match(r"jdbc:(?:mariadb|mysql)://([^:/]+):?(\d+)?/(.+)", jdbc_url)
#         if match:
#             server   = match.group(1)
#             port     = match.group(2) or "3306"
#             database = match.group(3)

#     # Fall back to explicit configuration variables if JDBC was absent or incomplete
#     if not server or not database:
#         server = os.getenv("TABAK_DB_SERVER", "").strip()
#         port = os.getenv("TABAK_DB_PORT", "3306").strip()
#         database = os.getenv("TABAK_DB_DATABASE", "").strip()

#     return {
#         "server":   server,
#         "port":     port,
#         "userid":   os.getenv("TABAK_DB_USERID", "").strip(),
#         "password": os.getenv("TABAK_DB_PASSWORD", "").strip(),
#         "database": database,
#     }

# def connect_database(creds):
#     """
#     Establishes connection to the target MariaDB database.
#     """
#     port = int(creds["port"])
#     try:
#         import pymysql
#         return pymysql.connect(
#             host=creds["server"], 
#             port=port, 
#             user=creds["userid"],
#             password=creds["password"], 
#             database=creds["database"], 
#             connect_timeout=10, 
#             charset="utf8mb4"
#         )
#     except ImportError:
#         print("❌ pymysql package is missing. Run: pip install pymysql")
#         return None
#     except Exception as e:
#         print(f"❌ DB connection failed: {e}")
#         return None

# def run_query(conn, sql: str, params: Optional[List[str]] = None):
#     """Executes a SQL query and returns fetched records."""
#     cursor = conn.cursor()
#     cursor.execute(sql, params or [])
#     rows = cursor.fetchall()
#     cursor.close()
#     return rows

# def clean_text(value):
#     return str(value).strip() if value is not None else ""

# def canonical_category(value):
#     """Maps varied category spelling structures to standard class designations."""
#     raw = clean_text(value)
#     key = raw.lower().replace("_", "").replace(" ", "")
#     mapping = {
#         "varatingdecision": "VA_Rating_Decision",
#         "vafeeletter": "VA_Fee_Letter",
#         "other": "Others",
#         "others": "Others",
#     }
#     return mapping.get(key, raw)

# def normalize_compare(val1, val2):
#     """Performs case-insensitive and whitespace-insensitive string matching."""
#     return val1.strip().lower() == val2.strip().lower()

# def is_prediction_correct(gen_cat, gen_sub, user_cat, user_sub):
#     """
#     Evaluates whether an individual transaction's prediction was correct.
#     - True if the user didn't make modifications (fields are empty).
#     - True if the user's manual adjustments match the AI's prediction exactly.
#     - False otherwise.
#     """
#     if not user_cat.strip() and not user_sub.strip():
#         return True
#     return normalize_compare(user_cat, gen_cat) and normalize_compare(user_sub, gen_sub)


# def validate_datetime(value: Optional[str]) -> Optional[datetime]:
#     if not value:
#         return None
#     return datetime.fromisoformat(value)


# def get_timeframe_sql(period):
#     """
#     Translates periods selected via --period CLI arguments to native 
#     MariaDB SQL date filtering snippets.
#     """
#     mapping = {
#         "15d": "AND file_receive_date >= NOW() - INTERVAL 15 DAY",
#         "1m":  "AND file_receive_date >= NOW() - INTERVAL 1 MONTH",
#         "1y":  "AND file_receive_date >= NOW() - INTERVAL 1 YEAR",
#         "mtd": "AND file_receive_date >= DATE_FORMAT(NOW(), '%Y-%m-01')",
#         "ytd": "AND file_receive_date >= DATE_FORMAT(NOW(), '%Y-01-01')",
#         "all": ""
#     }
#     return mapping.get(period, "")


# def build_query(
#     period: str,
#     start_datetime: Optional[datetime],
#     end_datetime: Optional[datetime],
# ) -> Tuple[str, List[str]]:
#     params: List[str] = []
#     filters = [
#         "template_info IS NOT NULL",
#         "template_info != ''",
#         "JSON_VALID(template_info)",
#         "JSON_EXISTS(template_info, '$.generated_response.VADetails.Category')",
#         "JSON_EXISTS(template_info, '$.generated_response.VADetails.Subcategory')",
#         "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Category')",
#         "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Subcategory')",
#     ]

#     period_filter = get_timeframe_sql(period)
#     if period_filter:
#         filters.append(period_filter.replace("AND ", "", 1))

#     if start_datetime:
#         filters.append("file_receive_date >= %s")
#         params.append(start_datetime.strftime("%Y-%m-%d %H:%M:%S"))

#     if end_datetime:
#         filters.append("file_receive_date < %s")
#         params.append(end_datetime.strftime("%Y-%m-%d %H:%M:%S"))

#     query = f"""
#         SELECT 
#                     file_receive_date,
#           JSON_VALUE(template_info, '$.generated_response.VADetails.Category') AS gen_cat,
#           JSON_VALUE(template_info, '$.generated_response.VADetails.Subcategory') AS gen_sub,
#           JSON_VALUE(template_info, '$.user_selected_response.VADetails.Category') AS user_cat,
#           JSON_VALUE(template_info, '$.user_selected_response.VADetails.Subcategory') AS user_sub
#         FROM `Transactions`
#         WHERE {' AND '.join(filters)}
#         ORDER BY file_receive_date ASC;
#     """
#     return query, params


# def compute_metrics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
#     total = len(results)
#     correct_count = sum(1 for item in results if item["is_correct"])

#     accuracy = (correct_count / total) * 100 if total else 0.0
#     cat_metrics = {category: {"TP": 0, "FP": 0, "FN": 0} for category in CATEGORIES}
#     sub_metrics: Dict[str, Dict[str, int]] = {}

#     for item in results:
#         gen_cat, gen_sub = item["gen_cat"], item["gen_sub"]
#         user_cat, user_sub = item["user_cat"], item["user_sub"]

#         actual_cat = user_cat if user_cat.strip() else gen_cat
#         actual_sub = user_sub if user_sub.strip() else gen_sub
#         pred_cat = gen_cat
#         pred_sub = gen_sub if pred_cat == "Others" else ""
#         act_sub = actual_sub if actual_cat == "Others" else ""

#         if pred_cat == actual_cat:
#             if pred_cat in cat_metrics:
#                 cat_metrics[pred_cat]["TP"] += 1
#         else:
#             if pred_cat in cat_metrics:
#                 cat_metrics[pred_cat]["FP"] += 1
#             if actual_cat in cat_metrics:
#                 cat_metrics[actual_cat]["FN"] += 1

#         if pred_cat == "Others" or actual_cat == "Others":
#             if pred_sub == act_sub:
#                 if pred_sub:
#                     if pred_sub not in sub_metrics:
#                         sub_metrics[pred_sub] = {"TP": 0, "FP": 0, "FN": 0}
#                     sub_metrics[pred_sub]["TP"] += 1
#             else:
#                 if pred_sub:
#                     if pred_sub not in sub_metrics:
#                         sub_metrics[pred_sub] = {"TP": 0, "FP": 0, "FN": 0}
#                     sub_metrics[pred_sub]["FP"] += 1
#                 if act_sub:
#                     if act_sub not in sub_metrics:
#                         sub_metrics[act_sub] = {"TP": 0, "FP": 0, "FN": 0}
#                     sub_metrics[act_sub]["FN"] += 1

#     category_rows: List[Dict[str, Any]] = []
#     for category in CATEGORIES:
#         values = cat_metrics[category]
#         precision = (
#             values["TP"] / (values["TP"] + values["FP"]) * 100
#             if (values["TP"] + values["FP"]) > 0
#             else 0.0
#         )
#         recall = (
#             values["TP"] / (values["TP"] + values["FN"]) * 100
#             if (values["TP"] + values["FN"]) > 0
#             else 0.0
#         )
#         category_rows.append(
#             {
#                 "category": category,
#                 "tp": values["TP"],
#                 "fp": values["FP"],
#                 "fn": values["FN"],
#                 "precision_pct": round(precision, 2),
#                 "recall_pct": round(recall, 2),
#             }
#         )

#     subcategory_rows: List[Dict[str, Any]] = []
#     for subcategory in sorted(sub_metrics):
#         values = sub_metrics[subcategory]
#         precision = (
#             values["TP"] / (values["TP"] + values["FP"]) * 100
#             if (values["TP"] + values["FP"]) > 0
#             else 0.0
#         )
#         recall = (
#             values["TP"] / (values["TP"] + values["FN"]) * 100
#             if (values["TP"] + values["FN"]) > 0
#             else 0.0
#         )
#         subcategory_rows.append(
#             {
#                 "subcategory": subcategory,
#                 "tp": values["TP"],
#                 "fp": values["FP"],
#                 "fn": values["FN"],
#                 "precision_pct": round(precision, 2),
#                 "recall_pct": round(recall, 2),
#             }
#         )

#     return {
#         "total": total,
#         "correct": correct_count,
#         "incorrect": total - correct_count,
#         "accuracy_pct": round(accuracy, 2),
#         "source_date_start": min((item["source_date"] for item in results), default=""),
#         "source_date_end": max((item["source_date"] for item in results), default=""),
#         "category_rows": category_rows,
#         "subcategory_rows": subcategory_rows,
#     }


# def _display_date(value: str) -> str:
#     try:
#         return datetime.fromisoformat(value).strftime("%m/%d/%Y")
#     except ValueError:
#         return value


# def _load_checkpoint_from_clickhouse() -> Optional[datetime]:
#     try:
#         return load_tabak_accuracy_checkpoint(ENVIRONMENT)
#     except Exception as ex:
#         print(f"[ClickHouse] Error loading checkpoint: {ex}")
#     return None

# def _upload_to_clickhouse(raw_rows: List[Dict[str, Any]]) -> None:
#     try:
#         ensure_tabak_raw_table()
#         insert_tabak_raw_data(
#             environment=ENVIRONMENT,
#             raw_rows=raw_rows
#         )
#         print("[ClickHouse] Upload completed successfully.")
#     except Exception as ex:
#         print(f"[ClickHouse] Error uploading results: {ex}")


# # ==========================================
# # Metric Printing
# # ==========================================

# def print_metrics(
#     metrics: Dict[str, Any],
#     period_label: str,
#     raw_rows: List[Dict[str, Any]],
# ):
#     """
#     Analyzes evaluation arrays, computes metrics, and prints:
#     1. Overall Classification Accuracy.
#     2. Category-Wise performance metrics.
#     3. Subcategory-Wise performance metrics under 'Others'.
#     4. Comprehensive List of Raw Eligible Records.
#     """
#     total = metrics["total"]
#     if total == 0:
#         print(f"\nNo valid rows found for period: {period_label}")
#         return

#     print("\n" + "=" * 70)
#     print(f"AUDIT ACCURACY SUMMARY [{period_label.upper()}]")
#     print("=" * 70)
#     print(f"Total evaluated records : {total}")
#     print(f"Correct predictions      : {metrics['correct']}")
#     print(f"Incorrect predictions    : {metrics['incorrect']}")
#     print("-" * 70)
#     print(f"OVERALL ACCURACY         : {metrics['accuracy_pct']:.2f}%")
#     print("=" * 70)

#     print("\nOVERALL CATEGORY PERFORMANCE")
#     print("-" * 76)
#     print(
#         f"{'Category':<22} | {'TP':^5} | {'FP':^5} | {'FN':^5} | {'Prec':^7} | {'Rec':^7}"
#     )
#     print("-" * 76)
#     for row in metrics["category_rows"]:
#         print(
#             f"{row['category']:<22} | {row['tp']:5d} | {row['fp']:5d} | {row['fn']:5d} | "
#             f"{row['precision_pct']:6.1f}% | {row['recall_pct']:6.1f}%"
#         )

#     if metrics["subcategory_rows"]:
#         print("\nOVERALL SUBCATEGORY PERFORMANCE (Others)")
#         print("-" * 86)
#         print(
#             f"{'Subcategory':<32} | {'TP':^5} | {'FP':^5} | {'FN':^5} | {'Prec':^7} | {'Rec':^7}"
#         )
#         print("-" * 86)
#         for row in metrics["subcategory_rows"]:
#             print(
#                 f"{row['subcategory'][:32]:<32} | {row['tp']:5d} | {row['fp']:5d} | {row['fn']:5d} | "
#                 f"{row['precision_pct']:6.1f}% | {row['recall_pct']:6.1f}%"
#             )

#     print("\nRAW ELIGIBLE RECORDS")
#     print("-" * 115)
#     print(
#         f"{'Date':<10} | {'Gen Category':<20} | {'Gen Subcategory':<20} | {'User Category':<20} | {'User Subcategory':<20} | {'Correct':<7}"
#     )
#     print("-" * 115)
#     for row in raw_rows:
#         is_correct_str = "Yes" if row["is_correct"] else "No"
#         print(
#             f"{_display_date(row['source_date']):<10} | "
#             f"{row['gen_cat'][:20]:<20} | "
#             f"{row['gen_sub'][:20]:<20} | "
#             f"{row['user_cat'][:20]:<20} | "
#             f"{row['user_sub'][:20]:<20} | "
#             f"{is_correct_str:<7}"
#         )
#     print("-" * 115)


# # ==========================================
# # Main Orchestrator
# # ==========================================

# def main():
#     # Setup CLI command arguments
#     parser = argparse.ArgumentParser(description="Evaluate live categorization audit accuracy from MariaDB.")
#     parser.add_argument("-p", "--period", choices=["15d", "1m", "1y", "mtd", "ytd", "all"], default="all",
#                         help="Specify the target timeframe to query.")
#     parser.add_argument("--start-datetime", help="Optional filter, ISO format: YYYY-MM-DDTHH:MM:SS")
#     parser.add_argument("--end-datetime", help="Optional filter, ISO format: YYYY-MM-DDTHH:MM:SS")
#     # Azure Functions injects worker-specific CLI args; ignore unknown options.
#     args, _ = parser.parse_known_args()

#     start_datetime = validate_datetime(args.start_datetime)
#     end_datetime = validate_datetime(args.end_datetime)

#     if not start_datetime and args.period == "all":
#         checkpoint = _load_checkpoint_from_clickhouse()
#         if checkpoint:
#             start_datetime = checkpoint
#             print(f"[Checkpoint] Using start_datetime for incremental fetch: {start_datetime.isoformat()}")
#         else:
#             print("[Checkpoint] No prior row found in ClickHouse. Fetching all data from beginning.")

#     creds = get_db_credentials()
#     conn = connect_database(creds)
#     if not conn: 
#         return 1

#     try:
#         query, query_params = build_query(args.period, start_datetime, end_datetime)
#         rows = run_query(conn, query, query_params)
#         print(f"✓ Connected. Period: {args.period}. Processed {len(rows)} matching records.")

#         # Structure query results sequentially with accuracy evaluation
#         results = []
#         for r in rows:
#             file_receive_date = r[0] # Keep the exact datetime for checkpointing
#             gen_cat = canonical_category(r[1])
#             gen_sub = clean_text(r[2])
#             user_cat = canonical_category(r[3])
#             user_sub = clean_text(r[4])
#             is_correct = is_prediction_correct(gen_cat, gen_sub, user_cat, user_sub)

#             results.append({
#                 "file_receive_date": file_receive_date,
#                 "source_date": file_receive_date.date().isoformat() if hasattr(file_receive_date, "date") else str(file_receive_date)[:10],
#                 "gen_cat": gen_cat,
#                 "gen_sub": gen_sub,
#                 "user_cat": user_cat,
#                 "user_sub": user_sub,
#                 "is_correct": is_correct
#             })

#         # Process overall metrics purely for CLI printing
#         metrics = compute_metrics(results)
#         print_metrics(metrics, args.period, results)

#         if metrics["total"] > 0:
#             if UPLOAD_TO_CLICKHOUSE:
#                 # Push the raw array directly to ClickHouse
#                 _upload_to_clickhouse(results)
#         else:
#             if UPLOAD_TO_CLICKHOUSE:
#                 print("[ClickHouse] No new rows to upload.")

#     finally:
#         conn.close()

# if __name__ == "__main__":
#     main()



"""
MariaDB Direct Audit Accuracy Evaluator
---------------------------------------
This script connects directly to a MariaDB database, extracts target audit fields 
utilizing database-level JSON capabilities, and calculates the overall and class-wise 
(TP, FP, FN) classification metrics over the raw eligible transactions.

It supports time-period filtering (--period), persists raw records to ClickHouse,
and infers incremental checkpoints from the latest saved file_receive_date.
"""

import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from clickhouse_store import (
    get_environment,
    insert_tabak_raw_data,
    load_tabak_accuracy_checkpoint,
    ensure_tabak_raw_table,
)

# Load database credentials and config from the environment (.env file)
ENV_PATH = Path(__file__).resolve().with_name(".env")
load_dotenv(dotenv_path=ENV_PATH)


UPLOAD_TO_CLICKHOUSE = True

ENVIRONMENT = get_environment()
CATEGORIES = ["VA_Rating_Decision", "VA_Fee_Letter", "Others"]


# ==========================================
# Database Connection & Query Functions
# ==========================================

def get_db_credentials():
    """
    Retrieves and parses database connection variables.
    Handles standard JDBC URLs or explicit environment variable fallbacks.
    """
    jdbc_url = os.getenv("TABAK_DB_JDBC_URL", "").strip()
    server, port, database = "", "3306", ""

    # Try parsing server and database out of a JDBC URL string if present
    if jdbc_url:
        match = re.match(r"jdbc:(?:mariadb|mysql)://([^:/]+):?(\d+)?/(.+)", jdbc_url)
        if match:
            server   = match.group(1)
            port     = match.group(2) or "3306"
            database = match.group(3)

    # Fall back to explicit configuration variables if JDBC was absent or incomplete
    if not server or not database:
        server = os.getenv("TABAK_DB_SERVER", "").strip()
        port = os.getenv("TABAK_DB_PORT", "3306").strip()
        database = os.getenv("TABAK_DB_DATABASE", "").strip()

    return {
        "server":   server,
        "port":     port,
        "userid":   os.getenv("TABAK_DB_USERID", "").strip(),
        "password": os.getenv("TABAK_DB_PASSWORD", "").strip(),
        "database": database,
    }

def connect_database(creds):
    """
    Establishes connection to the target MariaDB database.
    """
    port = int(creds["port"])
    try:
        import pymysql
        return pymysql.connect(
            host=creds["server"], 
            port=port, 
            user=creds["userid"],
            password=creds["password"], 
            database=creds["database"], 
            connect_timeout=10, 
            charset="utf8mb4"
        )
    except ImportError:
        print("❌ pymysql package is missing. Run: pip install pymysql")
        return None
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return None

def run_query(conn, sql: str, params: Optional[List[str]] = None):
    """Executes a SQL query and returns fetched records."""
    cursor = conn.cursor()
    cursor.execute(sql, params or [])
    rows = cursor.fetchall()
    cursor.close()
    return rows

def clean_text(value):
    return str(value).strip() if value is not None else ""

def canonical_category(value):
    """
    Maps varied category spelling structures to standard class designations.
    Both 'other' and 'others' map to the standardized 'Others'.
    """
    raw = clean_text(value)
    key = raw.lower().replace("_", "").replace(" ", "")
    mapping = {
        "varatingdecision": "VA_Rating_Decision",
        "vafeeletter": "VA_Fee_Letter",
        "other": "Others",
        "others": "Others",
    }
    return mapping.get(key, raw)

def normalize_compare(val1, val2):
    """Performs case-insensitive and whitespace-insensitive string matching."""
    return val1.strip().lower() == val2.strip().lower()


def validate_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


def get_timeframe_sql(period):
    """
    Translates periods selected via --period CLI arguments to native 
    MariaDB SQL date filtering snippets.
    """
    mapping = {
        "15d": "AND file_receive_date >= NOW() - INTERVAL 15 DAY",
        "1m":  "AND file_receive_date >= NOW() - INTERVAL 1 MONTH",
        "1y":  "AND file_receive_date >= NOW() - INTERVAL 1 YEAR",
        "mtd": "AND file_receive_date >= DATE_FORMAT(NOW(), '%Y-%m-01')",
        "ytd": "AND file_receive_date >= DATE_FORMAT(NOW(), '%Y-01-01')",
        "all": ""
    }
    return mapping.get(period, "")


def build_query(
    period: str,
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
) -> Tuple[str, List[str]]:
    params: List[str] = []
    filters = [
        "template_info IS NOT NULL",
        "template_info != ''",
        "JSON_VALID(template_info)",
        "JSON_EXISTS(template_info, '$.generated_response.VADetails.Category')",
        "JSON_EXISTS(template_info, '$.generated_response.VADetails.Subcategory')",
        "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Category')",
        "JSON_EXISTS(template_info, '$.user_selected_response.VADetails.Subcategory')",
    ]

    period_filter = get_timeframe_sql(period)
    if period_filter:
        filters.append(period_filter.replace("AND ", "", 1))

    if start_datetime:
        filters.append("file_receive_date >= %s")
        params.append(start_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    if end_datetime:
        filters.append("file_receive_date < %s")
        params.append(end_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    query = f"""
        SELECT 
                    file_receive_date,
          JSON_VALUE(template_info, '$.generated_response.VADetails.Category') AS gen_cat,
          JSON_VALUE(template_info, '$.generated_response.VADetails.Subcategory') AS gen_sub,
          JSON_VALUE(template_info, '$.user_selected_response.VADetails.Category') AS user_cat,
          JSON_VALUE(template_info, '$.user_selected_response.VADetails.Subcategory') AS user_sub
        FROM `Transactions`
        WHERE {' AND '.join(filters)}
        ORDER BY file_receive_date ASC;
    """
    return query, params


def compute_metrics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(results)
    correct_count = sum(1 for item in results if item["is_correct"])

    accuracy = (correct_count / total) * 100 if total else 0.0
    cat_metrics = {category: {"TP": 0, "FP": 0, "FN": 0} for category in CATEGORIES}
    sub_metrics: Dict[str, Dict[str, int]] = {}

    for item in results:
        gen_cat, gen_sub = item["gen_cat"], item["gen_sub"]
        user_cat, user_sub = item["user_cat"], item["user_sub"]

        actual_cat = user_cat if user_cat.strip() else gen_cat
        actual_sub = user_sub if user_sub.strip() else gen_sub
        pred_cat = gen_cat
        pred_sub = gen_sub if pred_cat == "Others" else ""
        act_sub = actual_sub if actual_cat == "Others" else ""

        # Category Metrics calculations
        if pred_cat == actual_cat:
            if pred_cat in cat_metrics:
                cat_metrics[pred_cat]["TP"] += 1
        else:
            if pred_cat in cat_metrics:
                cat_metrics[pred_cat]["FP"] += 1
            if actual_cat in cat_metrics:
                cat_metrics[actual_cat]["FN"] += 1

        # Subcategory Metrics calculations: Only processed when the category is correctly predicted as "Others"
        if pred_cat == "Others" and actual_cat == "Others":
            if pred_sub == act_sub:
                if pred_sub:
                    if pred_sub not in sub_metrics:
                        sub_metrics[pred_sub] = {"TP": 0, "FP": 0, "FN": 0}
                    sub_metrics[pred_sub]["TP"] += 1
            else:
                if pred_sub:
                    if pred_sub not in sub_metrics:
                        sub_metrics[pred_sub] = {"TP": 0, "FP": 0, "FN": 0}
                    sub_metrics[pred_sub]["FP"] += 1
                if act_sub:
                    if act_sub not in sub_metrics:
                        sub_metrics[act_sub] = {"TP": 0, "FP": 0, "FN": 0}
                    sub_metrics[act_sub]["FN"] += 1

    category_rows: List[Dict[str, Any]] = []
    for category in CATEGORIES:
        values = cat_metrics[category]
        precision = (
            values["TP"] / (values["TP"] + values["FP"]) * 100
            if (values["TP"] + values["FP"]) > 0
            else 0.0
        )
        recall = (
            values["TP"] / (values["TP"] + values["FN"]) * 100
            if (values["TP"] + values["FN"]) > 0
            else 0.0
        )
        category_rows.append(
            {
                "category": category,
                "tp": values["TP"],
                "fp": values["FP"],
                "fn": values["FN"],
                "precision_pct": round(precision, 2),
                "recall_pct": round(recall, 2),
            }
        )

    subcategory_rows: List[Dict[str, Any]] = []
    for subcategory in sorted(sub_metrics):
        values = sub_metrics[subcategory]
        precision = (
            values["TP"] / (values["TP"] + values["FP"]) * 100
            if (values["TP"] + values["FP"]) > 0
            else 0.0
        )
        recall = (
            values["TP"] / (values["TP"] + values["FN"]) * 100
            if (values["TP"] + values["FN"]) > 0
            else 0.0
        )
        subcategory_rows.append(
            {
                "subcategory": subcategory,
                "tp": values["TP"],
                "fp": values["FP"],
                "fn": values["FN"],
                "precision_pct": round(precision, 2),
                "recall_pct": round(recall, 2),
            }
        )

    return {
        "total": total,
        "correct": correct_count,
        "incorrect": total - correct_count,
        "accuracy_pct": round(accuracy, 2),
        "source_date_start": min((item["source_date"] for item in results), default=""),
        "source_date_end": max((item["source_date"] for item in results), default=""),
        "category_rows": category_rows,
        "subcategory_rows": subcategory_rows,
    }


def _display_date(value: str) -> str:
    try:
        return datetime.fromisoformat(value).strftime("%m/%d/%Y")
    except ValueError:
        return value


def _load_checkpoint_from_clickhouse() -> Optional[datetime]:
    try:
        return load_tabak_accuracy_checkpoint(ENVIRONMENT)
    except Exception as ex:
        print(f"[ClickHouse] Error loading checkpoint: {ex}")
    return None

def _upload_to_clickhouse(raw_rows: List[Dict[str, Any]]) -> None:
    try:
        ensure_tabak_raw_table()
        insert_tabak_raw_data(
            environment=ENVIRONMENT,
            raw_rows=raw_rows
        )
        print("[ClickHouse] Upload completed successfully.")
    except Exception as ex:
        print(f"[ClickHouse] Error uploading results: {ex}")


# ==========================================
# Metric Printing
# ==========================================

def print_metrics(
    metrics: Dict[str, Any],
    period_label: str,
    raw_rows: List[Dict[str, Any]],
):
    """
    Analyzes evaluation arrays, computes metrics, and prints:
    1. Overall Classification Accuracy.
    2. Category-Wise performance metrics.
    3. Subcategory-Wise performance metrics under 'Others'.
    4. Comprehensive List of Raw Eligible Records.
    """
    total = metrics["total"]
    if total == 0:
        print(f"\nNo valid rows found for period: {period_label}")
        return

    print("\n" + "=" * 70)
    print(f"AUDIT ACCURACY SUMMARY [{period_label.upper()}]")
    print("=" * 70)
    print(f"Total evaluated records : {total}")
    print(f"Correct predictions      : {metrics['correct']}")
    print(f"Incorrect predictions    : {metrics['incorrect']}")
    print("-" * 70)
    print(f"OVERALL ACCURACY         : {metrics['accuracy_pct']:.2f}%")
    print("=" * 70)

    print("\nOVERALL CATEGORY PERFORMANCE")
    print("-" * 76)
    print(
        f"{'Category':<22} | {'TP':^5} | {'FP':^5} | {'FN':^5} | {'Prec':^7} | {'Rec':^7}"
    )
    print("-" * 76)
    for row in metrics["category_rows"]:
        print(
            f"{row['category']:<22} | {row['tp']:5d} | {row['fp']:5d} | {row['fn']:5d} | "
            f"{row['precision_pct']:6.1f}% | {row['recall_pct']:6.1f}%"
        )

    if metrics["subcategory_rows"]:
        print("\nOVERALL SUBCATEGORY PERFORMANCE (Others)")
        print("-" * 86)
        print(
            f"{'Subcategory':<32} | {'TP':^5} | {'FP':^5} | {'FN':^5} | {'Prec':^7} | {'Rec':^7}"
        )
        print("-" * 86)
        for row in metrics["subcategory_rows"]:
            print(
                f"{row['subcategory'][:32]:<32} | {row['tp']:5d} | {row['fp']:5d} | {row['fn']:5d} | "
                f"{row['precision_pct']:6.1f}% | {row['recall_pct']:6.1f}%"
            )

    print("\nRAW ELIGIBLE RECORDS")
    print("-" * 125)
    print(
        f"{'Date':<10} | {'Gen Category':<18} | {'Gen Subcategory':<18} | {'User Category':<18} | {'User Subcategory':<18} | {'Corr Cat':<8} | {'Corr Sub':<8} | {'Correct':<7}"
    )
    print("-" * 125)
    for row in raw_rows:
        is_correct_str = "Yes" if row["is_correct"] else "No"
        corr_cat_str = "Yes" if row["correct_cat"] else "No"
        corr_sub_str = "Yes" if row["correct_sub_cat"] else "No"
        print(
            f"{_display_date(row['source_date']):<10} | "
            f"{row['gen_cat'][:18]:<18} | "
            f"{row['gen_sub'][:18]:<18} | "
            f"{row['user_cat'][:18]:<18} | "
            f"{row['user_sub'][:18]:<18} | "
            f"{corr_cat_str:<8} | "
            f"{corr_sub_str:<8} | "
            f"{is_correct_str:<7}"
        )
    print("-" * 125)


# ==========================================
# Main Orchestrator
# ==========================================

def main():
    # Setup CLI command arguments
    parser = argparse.ArgumentParser(description="Evaluate live categorization audit accuracy from MariaDB.")
    parser.add_argument("-p", "--period", choices=["15d", "1m", "1y", "mtd", "ytd", "all"], default="all",
                        help="Specify the target timeframe to query.")
    parser.add_argument("--start-datetime", help="Optional filter, ISO format: YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--end-datetime", help="Optional filter, ISO format: YYYY-MM-DDTHH:MM:SS")
    # Azure Functions injects worker-specific CLI args; ignore unknown options.
    args, _ = parser.parse_known_args()

    start_datetime = validate_datetime(args.start_datetime)
    end_datetime = validate_datetime(args.end_datetime)

    if not start_datetime and args.period == "all":
        checkpoint = _load_checkpoint_from_clickhouse()
        if checkpoint:
            start_datetime = checkpoint
            print(f"[Checkpoint] Using start_datetime for incremental fetch: {start_datetime.isoformat()}")
        else:
            print("[Checkpoint] No prior row found in ClickHouse. Fetching all data from beginning.")

    creds = get_db_credentials()
    conn = connect_database(creds)
    if not conn: 
        return 1

    try:
        query, query_params = build_query(args.period, start_datetime, end_datetime)
        rows = run_query(conn, query, query_params)
        print(f"✓ Connected. Period: {args.period}. Processed {len(rows)} matching records.")

        # Structure query results sequentially with accuracy evaluation
        results = []
        for r in rows:
            file_receive_date = r[0] # Keep the exact datetime for checkpointing
            gen_cat = canonical_category(r[1])
            gen_sub = clean_text(r[2])
            user_cat = canonical_category(r[3])
            user_sub = clean_text(r[4])

            # Resolve actual category context
            actual_cat = user_cat if user_cat.strip() else gen_cat

            # Evaluate correct_cat: True if empty (meaning accepted) or matching
            if not user_cat.strip():
                correct_cat = True
            else:
                correct_cat = normalize_compare(user_cat, gen_cat)

            # Evaluate correct_sub_cat: only evaluated if category scope is "Others" (or user changed it to "Others")
            if actual_cat == "Others" or gen_cat == "Others":
                if not user_sub.strip():
                    correct_sub_cat = True
                else:
                    correct_sub_cat = normalize_compare(user_sub, gen_sub)
            else:
                correct_sub_cat = True

            # Overall correctness logic
            is_correct = correct_cat and correct_sub_cat

            results.append({
                "file_receive_date": file_receive_date,
                "source_date": file_receive_date.date().isoformat() if hasattr(file_receive_date, "date") else str(file_receive_date)[:10],
                "gen_cat": gen_cat,
                "gen_sub": gen_sub,
                "user_cat": user_cat,
                "user_sub": user_sub,
                "correct_cat": correct_cat,
                "correct_sub_cat": correct_sub_cat,
                "is_correct": is_correct
            })

        # Process overall metrics purely for CLI printing
        metrics = compute_metrics(results)
        print_metrics(metrics, args.period, results)

        if metrics["total"] > 0:
            if UPLOAD_TO_CLICKHOUSE:
                # Push the raw array directly to ClickHouse
                _upload_to_clickhouse(results)
        else:
            if UPLOAD_TO_CLICKHOUSE:
                print("[ClickHouse] No new rows to upload.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()