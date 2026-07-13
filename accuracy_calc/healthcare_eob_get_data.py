import os
import pymysql
import logging
import json
import tempfile
import re
import datetime as dt
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pymysql.cursors import DictCursor
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ==========================================
# SETUP LOGGING
# ==========================================
DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "healthcare_eob_push.log")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", DEFAULT_LOG_FILE)
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

try:
    from langfuse import Langfuse
    logging.info("[Langfuse] Library imported successfully.")
except ImportError:
    logging.error("[Langfuse] FAILURE: Langfuse library not installed.")
    Langfuse = None

# ==========================================
# CONSTANTS & STRUCTURES
# ==========================================
FILE_STATUS_ENUM = {0: "Pending", 1: "Failed", 2: "Completed", 3: "ManuallyCreated", 4: "PartiallyCompleted"}

@dataclass
class CompareStats:
    matches: int = 0
    mismatches: int = 0

_EOB_ACTIVE_STATS: Optional[CompareStats] = None

# ==========================================
# DB HELPERS & PARSERS
# ==========================================
def q_ident(name: str) -> str: return f"`{name}`"

def resolve_table_name(conn: pymysql.connections.Connection, candidates: Sequence[str]) -> str:
    db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db
    with conn.cursor(DictCursor) as cur:
        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s", (db_name,))
        existing = {row["TABLE_NAME"].lower(): row["TABLE_NAME"] for row in cur.fetchall()}
    for candidate in candidates:
        if candidate.lower() in existing:
            return existing[candidate.lower()]
    raise RuntimeError(f"None of table candidates exist: {candidates}")

def fetch_rows_by_fk(conn, table: str, fk_name: str, fk_value: Any) -> list[dict[str, Any]]:
    with conn.cursor(DictCursor) as cur:
        cur.execute(f"SELECT * FROM {q_ident(table)} WHERE {q_ident(fk_name)}=%s ORDER BY Id", (fk_value,))
        return cur.fetchall()

def fetch_row_by_id(conn, table: str, row_id: Any) -> dict[str, Any] | None:
    with conn.cursor(DictCursor) as cur:
        cur.execute(f"SELECT * FROM {q_ident(table)} WHERE Id=%s", (row_id,))
        return cur.fetchone()

def parse_str(value: Any) -> str | None:
    if value is None: return None
    return str(value).strip() if str(value).strip() else None

def parse_int(value: Any) -> int | None:
    s = parse_str(value)
    if s is None: return None
    try: return int(s)
    except ValueError: return None

def parse_money(value: Any) -> Decimal | None:
    s = parse_str(value)
    if s is None: return None
    try: return Decimal(s.replace("$", "").replace(",", "").strip())
    except InvalidOperation: return None

def parse_date(value: Any) -> dt.date | None:
    s = parse_str(value)
    if s is None: return None
    for fmt in ["%m-%d-%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]:
        try: return dt.datetime.strptime(s, fmt).date()
        except ValueError: continue
    return None

def ocr_value(node: Any) -> str | None:
    if not isinstance(node, dict): return None
    val = node.get("value", node.get("Value"))
    return str(val) if val is not None else None

def to_date_db(value: Any) -> dt.date | None:
    if isinstance(value, (dt.datetime, dt.date)): return value if isinstance(value, dt.date) else value.date()
    return parse_date(value)

def to_decimal_db(value: Any) -> Decimal | None:
    if isinstance(value, Decimal): return value
    try: return Decimal(str(value))
    except InvalidOperation: return None

def _to_iso_date_string(val: Any) -> Optional[str]:
    if isinstance(val, (dt.datetime, dt.date)): return val.date().isoformat()
    return str(val).strip()[:10] if str(val).strip() and val else None

def _to_float_or_str(val: Any) -> Optional[Any]:
    return float(val) if isinstance(val, (Decimal, int, float)) else str(val) if val else None

def map_file_status(value: str | None) -> str:
    v = parse_str(value)
    if v in ("Manually Created", "ManuallyCreated"): return "ManuallyCreated"
    if v in ("Partially Completed", "PartiallyCompleted"): return "PartiallyCompleted"
    return v if v in FILE_STATUS_ENUM.values() else "Pending"

def normalize_status(value: Any) -> str | None:
    v = parse_str(value)
    if v and v.isdigit(): return FILE_STATUS_ENUM.get(int(v), v)
    return FILE_STATUS_ENUM.get(value, str(value)) if isinstance(value, int) else v

def normalize_for_compare(value: Any) -> Any:
    return None if isinstance(value, str) and not value.strip() else value

def value_str(value: Any) -> str:
    return "<null>" if value is None else str(value)

def eob_add_mismatch(message: str, mismatches: list[str]) -> None:
    global _EOB_ACTIVE_STATS
    mismatches.append(message)
    if _EOB_ACTIVE_STATS is not None: _EOB_ACTIVE_STATS.mismatches += 1

def eob_eq(path: str, actual: Any, expected: Any, mismatches: list[str]) -> None:
    global _EOB_ACTIVE_STATS
    if normalize_for_compare(actual) == normalize_for_compare(expected):
        if _EOB_ACTIVE_STATS is not None: _EOB_ACTIVE_STATS.matches += 1
    else:
        eob_add_mismatch(f"{path} mismatch. expected={value_str(expected)}, actual={value_str(actual)}", mismatches)

# ==========================================
# EOB AUDITING & GROUND TRUTH
# ==========================================
def audit_eob_allocation(allocation_row: dict, conn: pymysql.connections.Connection, tables: dict) -> list[str]:
    mismatches: list[str] = []
    raw_json = allocation_row.get("rawJson") or allocation_row.get("RawJson")
    if not raw_json: return ["rawJson missing"]
    try: root = json.loads(raw_json)
    except Exception as ex: return [f"JSON parse error: {ex}"]

    allocation = root.get("Allocation")
    if not isinstance(allocation, dict): return ["No Allocation object in JSON"]

    eob_eq("Allocation.File_name", allocation_row.get("File_name"), allocation.get("File_name"), mismatches)
    eob_eq("Allocation.Client", allocation_row.get("Client"), allocation.get("Client"), mismatches)
    eob_eq("Allocation.Account", allocation_row.get("Account"), allocation.get("Account"), mismatches)
    eob_eq("Allocation.Total_No_Of_Clm_On_File", allocation_row.get("Total_No_Of_Clm_On_File"), allocation.get("Total_No_Of_Clm_On_File"), mismatches)
    eob_eq("Allocation.Total_Paid_Amt_On_File", to_decimal_db(allocation_row.get("Total_Paid_Amt_On_File")), parse_money(allocation.get("Total_Paid_Amt_On_File")), mismatches)
    eob_eq("Allocation.File_Status", normalize_status(allocation_row.get("File_Status")), map_file_status(allocation.get("File_Status")), mismatches)

    expected_claims = [c.get("Claim") for c in allocation.get("Claims_Info") or [] if isinstance(c, dict) and isinstance(c.get("Claim"), dict)]
    db_claims = fetch_rows_by_fk(conn, tables["claim"], "AllocationId", allocation_row["Id"])
    if len(db_claims) != len(expected_claims): eob_add_mismatch(f"Claims count mismatch: exp {len(expected_claims)}, actual {len(db_claims)}", mismatches)

    for idx, (db_claim, claim_obj) in enumerate(zip(db_claims, expected_claims)):
        p = f"Claim[{idx}]"
        eob_eq(f"{p}.Claim_Number", db_claim.get("Claim_Number"), parse_str(ocr_value(claim_obj.get("Claim_Number"))), mismatches)
        eob_eq(f"{p}.Claim_Total_Charge_Amt", to_decimal_db(db_claim.get("Claim_Total_Charge_Amt")), parse_money(ocr_value(claim_obj.get("Claim_Total_Charge_Amt"))), mismatches)
        
        # Payer
        exp_payer = claim_obj.get("Payer")
        db_payer = fetch_row_by_id(conn, tables["payer"], db_claim.get("PayerId")) if db_claim.get("PayerId") else None
        if isinstance(exp_payer, dict) and db_payer:
            eob_eq(f"{p}.Payer.Payer_Name", db_payer.get("Payer_Name"), parse_str(ocr_value(exp_payer.get("Payer_Name"))), mismatches)
        
        # Payee
        exp_payee = claim_obj.get("Payee")
        db_payee = fetch_row_by_id(conn, tables["payee"], db_claim.get("PayeeId")) if db_claim.get("PayeeId") else None
        if isinstance(exp_payee, dict) and db_payee:
            eob_eq(f"{p}.Payee.Payee_Name", db_payee.get("Payee_Name"), parse_str(ocr_value(exp_payee.get("Payee_Name"))), mismatches)

        # Service Lines
        exp_sls = claim_obj.get("Service_Line_Items") or []
        db_sls = fetch_rows_by_fk(conn, tables["service_line"], "ClaimId", db_claim["Id"])
        for sl_idx, (db_sl, sl_obj) in enumerate(zip(db_sls, exp_sls)):
            ps = f"{p}.SL[{sl_idx}]"
            eob_eq(f"{ps}.Procedure_Code", db_sl.get("Procedure_Code"), parse_str(ocr_value(sl_obj.get("Procedure_Code"))), mismatches)
            eob_eq(f"{ps}.Service_Billed_Amt", to_decimal_db(db_sl.get("Service_Billed_Amt")), parse_money(ocr_value(sl_obj.get("Service_Billed_Amt"))), mismatches)
    return mismatches

def _get_eob_ground_truth_dict(row: dict, conn: pymysql.connections.Connection, tables: dict) -> dict:
    """Builds the complete structured ground truth dictionary from EOB relational database tables."""
    claims_rows = fetch_rows_by_fk(conn, tables["claim"], "AllocationId", row.get("Id"))
    claims_list = []
    
    for claim in claims_rows:
        claim_id = claim.get("Id")
        payer = fetch_row_by_id(conn, tables["payer"], claim.get("PayerId")) if claim.get("PayerId") else None
        payee = fetch_row_by_id(conn, tables["payee"], claim.get("PayeeId")) if claim.get("PayeeId") else None
        patient = fetch_row_by_id(conn, tables["patient"], claim.get("PatientId")) if claim.get("PatientId") else None
        
        dx_rows = fetch_rows_by_fk(conn, tables["diagnosis"], "ClaimId", claim_id)
        dx = dx_rows[0] if dx_rows else None
        
        sl_rows = fetch_rows_by_fk(conn, tables["service_line"], "ClaimId", claim_id)
        service_lines_list = []
        for sl in sl_rows:
            sl_id = sl.get("Id")
            adj_rows = fetch_rows_by_fk(conn, tables["service_adjustment"], "ServiceLineItemId", sl_id)
            
            service_lines_list.append({
                "Service_From_Date": _to_iso_date_string(sl.get("Service_From_Date")),
                "Service_To_Date": _to_iso_date_string(sl.get("Service_To_Date")),
                "Procedure_Code": sl.get("Procedure_Code"),
                "Mod1": sl.get("Mod1"),
                "Mod2": sl.get("Mod2"),
                "Mod3": sl.get("Mod3"),
                "Mod4": sl.get("Mod4"),
                "Service_Billed_Amt": _to_float_or_str(sl.get("Service_Billed_Amt")),
                "Service_Allowed_Amt": _to_float_or_str(sl.get("Service_Allowed_Amt")),
                "Service_Paid_Amt": _to_float_or_str(sl.get("Service_Paid_Amt")),
                "D_U": sl.get("D_U"),
                "Place_Of_Service": sl.get("Place_Of_Service"),
                "DX_1": sl.get("DX_1"),
                "DX_2": sl.get("DX_2"),
                "DX_3": sl.get("DX_3"),
                "DX_4": sl.get("DX_4"),
                "User_Status": sl.get("User_Status"),
                "Service_Adjustments": [
                    {
                        "Service_Adjustment_Reason_Code": adj.get("Service_Adjustment_Reason_Code"),
                        "Service_Adjustment_Group_Code": adj.get("Service_Adjustment_Group_Code"),
                        "Service_Adjustment_Reason": adj.get("Service_Adjustment_Reason"),
                        "Service_Adjustment_Amount": _to_float_or_str(adj.get("Service_Adjustment_Amount"))
                    } for adj in adj_rows
                ]
            })
            
        claims_list.append({
            "Claim_Number": claim.get("Claim_Number"),
            "Claim_Total_Charge_Amt": _to_float_or_str(claim.get("Claim_Total_Charge_Amt")),
            "Claim_Paid_Amt": _to_float_or_str(claim.get("Claim_Paid_Amt")),
            "Claim_Status_Code": claim.get("Claim_Status_Code"),
            "Claim_Status_Reason": claim.get("Claim_Status_Reason"),
            "Claim_Facility_Type": claim.get("Claim_Facility_Type"),
            "Claim_Frequency": claim.get("Claim_Frequency"),
            "Claim_Date_of_Service_From": _to_iso_date_string(claim.get("Claim_Date_of_Service_From")),
            "Claim_Date_of_Service_To": _to_iso_date_string(claim.get("Claim_Date_of_Service_To")),
            "Claim_Recieved_Date": _to_iso_date_string(claim.get("Claim_Recieved_Date")),
            "Patient_Responsibility_Amt": _to_float_or_str(claim.get("Patient_Responsibility_Amt")),
            "CLIA_Number": claim.get("CLIA_Number"),
            "Claim_Invoice_Number": claim.get("Claim_Invoice_Number"),
            "Admission_Date": _to_iso_date_string(claim.get("Admission_Date")),
            "Patient_Last_Seen_Date": _to_iso_date_string(claim.get("Patient_Last_Seen_Date")),
            "Payer": {
                "Payer_Name": payer.get("Payer_Name") if payer else None,
                "Payer_Id": payer.get("Payer_Id") if payer else None,
                "Payer_Addr1": payer.get("Payer_Addr1") if payer else None,
                "Payer_Addr2": payer.get("Payer_Addr2") if payer else None,
                "Payer_City": payer.get("Payer_City") if payer else None,
                "Payer_State": payer.get("Payer_State") if payer else None,
                "Payer_Zip": payer.get("Payer_Zip") if payer else None
            },
            "Payee": {
                "Payee_Name": payee.get("Payee_Name") if payee else None,
                "Payee_NPI": payee.get("Payee_NPI") if payee else None,
                "Payee_TaxID": payee.get("Payee_TaxID") if payee else None,
                "Payee_Addr1": payee.get("Payee_Addr1") if payee else None,
                "Payee_Addr2": payee.get("Payee_Addr2") if payee else None,
                "Payee_City": payee.get("Payee_City") if payee else None,
                "Payee_State": payee.get("Payee_State") if payee else None,
                "Payee_Zip": payee.get("Payee_Zip") if payee else None,
                "Rendering_Provider_Name": payee.get("Rendering_Provider_Name") if payee else None,
                "Rendering_Provider_NPI": payee.get("Rendering_Provider_NPI") if payee else None,
                "Check_EFT_Number": payee.get("Check_EFT_Number") if payee else None,
                "Payment_Amt": _to_float_or_str(payee.get("Payment_Amt")) if payee else None,
                "Check_Date": _to_iso_date_string(payee.get("Check_Date")) if payee else None
            },
            "Patient": {
                "Patient_FN": patient.get("Patient_FN") if patient else None,
                "Patient_LN": patient.get("Patient_LN") if patient else None,
                "Patient_MI": patient.get("Patient_MI") if patient else None,
                "Patient_Id": patient.get("Patient_Id") if patient else None,
                "Patient_Control_Number": patient.get("Patient_Control_Number") if patient else None,
                "Patient_Group": patient.get("Patient_Group") if patient else None,
                "Patient_Addr1": patient.get("Patient_Addr1") if patient else None,
                "Patient_Addr2": patient.get("Patient_Addr2") if patient else None,
                "Patient_City": patient.get("Patient_City") if patient else None,
                "Patient_State": patient.get("Patient_State") if patient else None,
                "Patient_Zip": patient.get("Patient_Zip") if patient else None,
                "Patient_DOB": _to_iso_date_string(patient.get("Patient_DOB")) if patient else None,
                "Patient_Gender": patient.get("Patient_Gender") if patient else None,
                "Patient_Relationship": patient.get("Patient_Relationship") if patient else None,
                "Insured_Name": patient.get("Insured_Name") if patient else None
            },
            "Claim_Diagnosis": {
                "Primary_DX": dx.get("Primary_DX") if dx else None,
                **{f"Secondary_DX{i}": dx.get(f"Secondary_DX{i}") if dx else None for i in range(1, 13)}
            },
            "Service_Line_Items": service_lines_list
        })

    return {
        "Allocation": {
            "File_name": row.get("File_name"),
            "File_url": row.get("File_url"),
            "Client": row.get("Client"),
            "Account": row.get("Account"),
            "Total_No_Of_Clm_On_File": row.get("Total_No_Of_Clm_On_File"),
            "Total_Paid_Amt_On_File": _to_float_or_str(row.get("Total_Paid_Amt_On_File")),
            "Check_Date": _to_iso_date_string(row.get("Check_Date")),
            "Total_Denied_Claims": row.get("Total_Denied_Claims"),
            "Total_Denied_Lines": row.get("Total_Denied_Lines"),
            "Total_Posted_Claims": row.get("Total_Posted_Claims"),
            "Download_Date": _to_iso_date_string(row.get("Download_Date")),
            "Completed_Date": _to_iso_date_string(row.get("Completed_Date")),
            "Not_Completed_Reasons": row.get("Not_Completed_Reasons"),
            "File_Status": row.get("File_Status")
        },
        "Claims_Info": [{"Claim": c} for c in claims_list]
    }
def evaluate_eob_record(row: dict, conn: pymysql.connections.Connection, tables: dict) -> dict:
    global _EOB_ACTIVE_STATS
    _EOB_ACTIVE_STATS = CompareStats()
    try: mismatches = audit_eob_allocation(row, conn, tables)
    except Exception as ex: mismatches = [f"Audit err: {ex}"]
    stats = _EOB_ACTIVE_STATS or CompareStats()
    _EOB_ACTIVE_STATS = None
    
    total = stats.matches + len(mismatches)
    acc = (stats.matches / total * 100.0) if total > 0 else 0.0
    
    try: gt_dict = _get_eob_ground_truth_dict(row, conn, tables)
    except Exception: gt_dict = {}

    return {
        "filename": row.get("File_name", ""),
        "client_name": row.get("Client", ""),
        "rawjson": row.get("rawJson") or row.get("RawJson") or "",
        "ground_truth": gt_dict,
        "total_matches": stats.matches,
        "total_mismatches": len(mismatches),
        "accuracy_percentage": round(acc, 2)
    }

# ==========================================
# LANGFUSE & AZURE SB UTILITIES
# ==========================================
def _get_langfuse_client():
    if not Langfuse: return None
    pk, sk, host = os.getenv("LANGFUSE_PUBLIC_KEY"), os.getenv("LANGFUSE_SECRET_KEY"), os.getenv("LANGFUSE_HOST")
    if not all([pk, sk, host]): return None
    return Langfuse(public_key=pk.strip(), secret_key=sk.strip(), host=host.strip())

def _load_checkpoint(dataset_name: str) -> Optional[str]:
    lf = _get_langfuse_client()
    if not lf: return None
    try:
        dataset = lf.get_dataset(dataset_name)
        if not dataset.items: return None
        latest = max(dataset.items, key=lambda r: getattr(r, 'created_at'))
        return str(latest.input.get("last_id")) if isinstance(latest.input, dict) else None
    except Exception: return None

def _save_checkpoint(dataset_name: str, env: str, last_id: str):
    lf = _get_langfuse_client()
    if not lf: return
    try:
        lf.create_dataset(name=dataset_name, description=f"EOB Checkpoint ({env})")
        lf.create_dataset_item(
            dataset_name=dataset_name,
            id=f"{dataset_name}::checkpoint::id::{last_id}",
            input={"last_id": last_id, "saved_at": datetime.now(timezone.utc).isoformat()}
        )
        lf.flush()
    except Exception as e: logging.error(f"Checkpoint save fail: {e}")

def _save_predictions(dataset_name: str, env: str, records: List[Dict]):
    lf = _get_langfuse_client()
    if not lf: return
    try:
        lf.create_dataset(name=dataset_name, description=f"EOB Predictions ({env})")
        for rec in records:
            lf.create_dataset_item(
                dataset_name=dataset_name,
                input={k: rec[k] for k in ["id", "filename", "client_name", "rawjson", "ground_truth", "total_matches", "total_mismatches", "accuracy_percentage"]},
                metadata={"record_id": rec["id"], "source": "EOB", "environment": env}
            )
        lf.flush()
    except Exception as e: logging.error(f"Predictions upload fail: {e}")

def _send_to_azure_queue(queue_name: str, messages: List[str]):
    conn_str = os.getenv("SERVICE_BUS_CONNECTION_STRING")
    if not conn_str: return
    with ServiceBusClient.from_connection_string(conn_str) as client:
        with client.get_queue_sender(queue_name=queue_name) as sender:
            for msg in messages: sender.send_messages(ServiceBusMessage(msg))

# ==========================================
# MAIN EOB PIPELINE
# ==========================================
def healthcare_eob_push(ids_per_message: int = 5, max_messages: int = 10):
    env = os.getenv("LANGFUSE_ENVIRONMENT", "dev").strip()
    checkpoint_ds = f"healthcare_accuracy_eob_{env}"
    predictions_ds = f"healthcare_predictions_eob_{env}"
    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "healthcare-accuracy-queue")
    save_preds = os.getenv("SAVE_PREDICTIONS_DATASET", "TRUE").upper() == "TRUE"

    conn = pymysql.connect(
        host=os.getenv("HEALTHCARE_AI_DB_SERVER"),
        port=int(os.getenv("HEALTHCARE_AI_DB_PORT", 3306)),
        user=os.getenv("HEALTHCARE_AI_DB_USERID"),
        password=os.getenv("HEALTHCARE_AI_DB_PASSWORD"),
        database=os.getenv("HEALTHCARE_AI_DB_DATABASE"),
        charset="utf8mb4"
    )

    try:
        tables = {
            "allocation": resolve_table_name(conn, ["EOB_Allocation", "EOBAllocations"]),
            "claim": resolve_table_name(conn, ["EOB_Claim", "EOBClaims"]),
            "payer": resolve_table_name(conn, ["EOB_Payer", "EOBPayers"]),
            "payee": resolve_table_name(conn, ["EOB_Payee", "EOBPayees"]),
            "patient": resolve_table_name(conn, ["PatientRecord", "PatientRecords"]),
            "diagnosis": resolve_table_name(conn, ["EOB_Claim_Diagnosis", "EOBClaim_Diagnosis"]),
            "service_line": resolve_table_name(conn, ["EOB_Service_Line_Item", "EOBService_Line_Items"]),
            "service_adjustment": resolve_table_name(conn, ["EOB_Service_Adjustment", "EOBService_Adjustments"])
        }

        last_id = _load_checkpoint(checkpoint_ds)
        query = f"SELECT Id, rawJson FROM `{tables['allocation']}` WHERE rawJson IS NOT NULL AND rawJson != ''"
        params = []
        if last_id:
            query += " AND Id > %s"
            params.append(last_id)
        query += " ORDER BY Id ASC"

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = [{"Id": r[0], "rawJson": r[1]} for r in cur.fetchall()]

        records = []
        for row in rows:
            eval_metrics = evaluate_eob_record(row, conn, tables)
            records.append({"id": str(row["Id"]), **eval_metrics})

        if records:
            chunks = [records[i:i + ids_per_message] for i in range(0, len(records), ids_per_message)][:max_messages]
            msgs = [json.dumps({"record_ids": [r["id"] for r in chunk], "source": "healthcare_eob"}) for chunk in chunks]
            
            _send_to_azure_queue(queue_name, msgs)
            _save_checkpoint(checkpoint_ds, env, chunks[-1][-1]["id"])
            if save_preds:
                _save_predictions(predictions_ds, env, [r for c in chunks for r in c])
            logging.info(f"Processed {len(chunks)} chunks of EOB records.")

    finally:
        conn.close()

if __name__ == "__main__":
    healthcare_eob_push()