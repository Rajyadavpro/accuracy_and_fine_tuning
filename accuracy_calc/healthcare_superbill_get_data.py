import os
import pymysql
import logging
import json
import tempfile
import re
import threading
import datetime as dt
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pymysql.cursors import DictCursor
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ==========================================
# SETUP LOGGING
# ==========================================
DEFAULT_LOG_FILE = os.path.join(tempfile.gettempdir(), "healthcare_superbill_push.log")
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

_SB_ACTIVE_STATS = threading.local()

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

def parse_money(value: Any) -> Decimal | None:
    s = parse_str(value)
    if s is None: return None
    try: return Decimal(s.replace("$", "").replace(",", "").strip())
    except InvalidOperation: return None

def ocr_value(node: Any) -> str | None:
    if not isinstance(node, dict): return None
    val = node.get("value", node.get("Value"))
    return str(val) if val is not None else None

def to_decimal_db(value: Any) -> Decimal | None:
    if isinstance(value, Decimal): return value
    try: return Decimal(str(value))
    except InvalidOperation: return None

def _to_float_or_str(val: Any) -> Optional[Any]:
    return float(val) if isinstance(val, (Decimal, int, float)) else str(val) if val else None

def normalize_compare_str(value: str) -> str:
    return " ".join(value.strip().split()).casefold()

def value_str(value: Any) -> str:
    return "<null>" if value is None else str(value)

def sb_add_mismatch(message: str, mismatches: list[str]) -> None:
    mismatches.append(message)
    stats = getattr(_SB_ACTIVE_STATS, "value", None)
    if stats: stats.mismatches += 1

def sb_eq(path: str, actual: Any, expected: Any, mismatches: list[str]) -> None:
    stats = getattr(_SB_ACTIVE_STATS, "value", None)
    if actual == expected:
        if stats: stats.matches += 1
        return
    if isinstance(actual, str) and isinstance(expected, str) and normalize_compare_str(actual) == normalize_compare_str(expected):
        if stats: stats.matches += 1
        return
    sb_add_mismatch(f"{path} mismatch. expected={value_str(expected)}, actual={value_str(actual)}", mismatches)

# ==========================================
# SUPERBILL AUDITING & GROUND TRUTH
# ==========================================
def audit_superbill_allocation(allocation_row: dict, conn: pymysql.connections.Connection, tables: dict) -> tuple[list[str], CompareStats]:
    mismatches = []
    stats = CompareStats()
    _SB_ACTIVE_STATS.value = stats

    raw_json = allocation_row.get("RawJson") or allocation_row.get("rawJson")
    if not raw_json:
        sb_add_mismatch("RawJson missing", mismatches)
        return mismatches, stats

    try: root = json.loads(raw_json)
    except Exception as ex:
        sb_add_mismatch(f"JSON err: {ex}", mismatches)
        return mismatches, stats

    allocation = root.get("Allocation")
    if not isinstance(allocation, dict): return mismatches, stats

    sb_eq("Allocation.File_name", allocation_row.get("File_name"), allocation.get("File_name"), mismatches)
    sb_eq("Allocation.Client", allocation_row.get("Client"), allocation.get("Client"), mismatches)
    sb_eq("Allocation.Total_No_Of_Clm_On_File", allocation_row.get("Total_No_Of_Clm_On_File"), len(allocation.get("Claim_Info") or []), mismatches)
    
    exp_claims = [c.get("Claim") for c in allocation.get("Claim_Info") or [] if isinstance(c, dict) and isinstance(c.get("Claim"), dict)]
    db_claims = fetch_rows_by_fk(conn, tables["claim"], "AllocationRecordId", allocation_row["Id"])

    if len(db_claims) != len(exp_claims):
        sb_add_mismatch(f"Claims count mismatch: exp {len(exp_claims)}, act {len(db_claims)}", mismatches)

    for idx, (db_claim, claim_obj) in enumerate(zip(db_claims, exp_claims)):
        p = f"Claim[{idx}]"
        sb_eq(f"{p}.Patient_Control_Number", db_claim.get("Patient_Control_Number"), parse_str(ocr_value(claim_obj.get("Patient_Control_Number"))), mismatches)
        sb_eq(f"{p}.Claim_Total_Charge_Amt", to_decimal_db(db_claim.get("Claim_Total_Charge_Amt")), parse_money(ocr_value(claim_obj.get("Claim_Total_Charge_Amt"))), mismatches)
        
        # Payer Check
        exp_payers = claim_obj.get("Payer") or []
        db_payers = fetch_rows_by_fk(conn, tables["payer"], "ClaimRecordId", db_claim["Id"])
        for p_idx, (db_payer, payer_obj) in enumerate(zip(db_payers, exp_payers)):
            sb_eq(f"{p}.Payer[{p_idx}].Payer_Name", db_payer.get("Payer_Name"), parse_str(ocr_value((payer_obj or {}).get("Payer_Name"))), mismatches)

        # Service Lines
        exp_sls = claim_obj.get("ServiceLines") or []
        db_sls = fetch_rows_by_fk(conn, tables["sl"], "ClaimRecordId", db_claim["Id"])
        for s_idx, (db_sl, sl_obj) in enumerate(zip(db_sls, exp_sls)):
            sb_eq(f"{p}.SL[{s_idx}].Procedure_Code", db_sl.get("Procedure_Code"), parse_str(ocr_value((sl_obj or {}).get("Procedure_Code"))), mismatches)

    _SB_ACTIVE_STATS.value = None
    return mismatches, stats

def _get_superbill_ground_truth_dict(row: dict, conn: pymysql.connections.Connection, tables: dict) -> dict:
    """Builds the complete structured ground truth dictionary from Superbill relational database tables."""
    claims_rows = fetch_rows_by_fk(conn, tables["claim"], "AllocationRecordId", row.get("Id"))
    claims_list = []
    
    for claim in claims_rows:
        claim_id = claim.get("Id")
        patient = fetch_row_by_id(conn, tables["patient"], claim.get("PatientId")) if claim.get("PatientId") else None
        payer_rows = fetch_rows_by_fk(conn, tables["payer"], "ClaimRecordId", claim_id)
        provider_rows = fetch_rows_by_fk(conn, tables["provider"], "ClaimRecordId", claim_id)
        dx_rows = fetch_rows_by_fk(conn, tables["dx"], "ClaimRecordId", claim_id)
        dx = dx_rows[0] if dx_rows else None
        sl_rows = fetch_rows_by_fk(conn, tables["sl"], "ClaimRecordId", claim_id)
        
        providers_dict = {}
        for prov in provider_rows:
            role = prov.get("Role")
            if role:
                providers_dict[role] = {
                    "Provider_Name": prov.get("Provider_Name"),
                    "Provider_NPI": prov.get("Provider_NPI"),
                    "Provider_Addr1": prov.get("Provider_Addr1"),
                    "Provider_Addr2": prov.get("Provider_Addr2"),
                    "Provider_City": prov.get("Provider_City"),
                    "Provider_State": prov.get("Provider_State"),
                    "Provider_Zip": prov.get("Provider_Zip"),
                    "Provider_FedId": prov.get("Provider_FedId"),
                    "Provider_TaxId": prov.get("Provider_TaxId"),
                    "Provider_Taxonomy": prov.get("Provider_Taxonomy")
                }

        claims_list.append({
            "Patient_Control_Number": claim.get("Patient_Control_Number"),
            "Claim_Total_Charge_Amt": _to_float_or_str(claim.get("Claim_Total_Charge_Amt")),
            "Claim_Filing_indicator": claim.get("Claim_Filing_indicator"),
            "Claim_Frequency_Code": claim.get("Claim_Frequency_Code"),
            "Claim_Date_of_Service": _to_iso_date_string(claim.get("Claim_Date_of_Service")),
            "Claim_Auth_No": claim.get("Claim_Auth_No"),
            "Patient_Paid_Amt": _to_float_or_str(claim.get("Patient_Paid_Amt")),
            "CLIA_Number": claim.get("CLIA_Number"),
            "CLaim_Invoice_Number": claim.get("CLaim_Invoice_Number"),
            "Admission_Date": _to_iso_date_string(claim.get("Admission_Date")),
            "Patient_Last_Seen_Date": _to_iso_date_string(claim.get("Patient_Last_Seen_Date")),
            "Patient": {
                "Patient_FN": patient.get("Patient_FN") if patient else None,
                "Patient_LN": patient.get("Patient_LN") if patient else None,
                "Patient_MI": patient.get("Patient_MI") if patient else None,
                "Patient_Id": patient.get("Patient_Id") if patient else None,
                "Patient_Account_Number": patient.get("Patient_Account_Number") if patient else None,
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
                "Patient_Marital_Status": patient.get("Patient_Marital_Status") if patient else None,
                "Patient_Primary_Phone_No": patient.get("Patient_Primary_Phone_No") if patient else None,
                "Patient_Home_Phone_No": patient.get("Patient_Home_Phone_No") if patient else None,
                "Patient_Primary_Email": patient.get("Patient_Primary_Email") if patient else None,
                "Insured_Name": patient.get("Insured_Name") if patient else None
            },
            "Payer": [
                {
                    "Payer_Name": p.get("Payer_Name"),
                    "Payer_Id": p.get("Payer_Id"),
                    "Payer_Addr1": p.get("Payer_Addr1"),
                    "Payer_Addr2": p.get("Payer_Addr2"),
                    "Payer_City": p.get("Payer_City"),
                    "Payer_State": p.get("Payer_State"),
                    "Payer_Zip": p.get("Payer_Zip"),
                    "Payer_Type": p.get("Payer_Type")
                }
                for p in payer_rows
            ],
            **providers_dict,
            "ClaimDiagnosisCodes": {
                "Primary_DX": dx.get("Primary_DX") if dx else None,
                **{f"Secondary_DX{i}": dx.get(f"Secondary_DX{i}") if dx else None for i in range(1, 13)}
            },
            "ServiceLines": [
                {
                    "Service_From_Date": _to_iso_date_string(sl.get("Service_From_Date")),
                    "Service_To_Date": _to_iso_date_string(sl.get("Service_To_Date")),
                    "Procedure_Code": sl.get("Procedure_Code"),
                    "Mod1": sl.get("Mod1"),
                    "Mod2": sl.get("Mod2"),
                    "Mod3": sl.get("Mod3"),
                    "Mod4": sl.get("Mod4"),
                    "Service_Billed_Amt": _to_float_or_str(sl.get("Service_Billed_Amt")),
                    "D_U": sl.get("D_U"),
                    "Place_Of_Service": sl.get("Place_Of_Service"),
                    "DX_1": sl.get("DX_1"),
                    "DX_2": sl.get("DX_2"),
                    "DX_3": sl.get("DX_3"),
                    "DX_4": sl.get("DX_4"),
                    "User_Status": sl.get("User_Status")
                }
                for sl in sl_rows
            ]
        })

    return {
        "Allocation": {
            "File_name": row.get("File_name"),
            "File_url": row.get("File_url"),
            "Client": row.get("Client"),
            "Account": row.get("Account"),
            "Total_No_Of_Clm_On_File": row.get("Total_No_Of_Clm_On_File"),
            "Total_Charge_Amt_On_File": _to_float_or_str(row.get("Total_Charge_Amt_On_File")),
            "Date_Of_Service": _to_iso_date_string(row.get("Date_Of_Service")),
            "Download_Date": _to_iso_date_string(row.get("Download_Date")),
            "Completed_Date": _to_iso_date_string(row.get("Completed_Date")),
            "Not_Completed_Reason": row.get("Not_Completed_Reason"),
            "File_Status": row.get("File_Status")
        },
        "Claim_Info": [{"Claim": c} for c in claims_list]
    }
def evaluate_superbill_record(row: dict, conn: pymysql.connections.Connection, tables: dict) -> dict:
    try: mismatches, stats = audit_superbill_allocation(row, conn, tables)
    except Exception as ex:
        mismatches, stats = [f"Audit err: {ex}"], CompareStats()
    
    total = stats.matches + len(mismatches)
    acc = (stats.matches / total * 100.0) if total > 0 else 0.0
    
    try: gt_dict = _get_superbill_ground_truth_dict(row, conn, tables)
    except Exception: gt_dict = {}

    return {
        "filename": row.get("File_name", ""),
        "client_name": row.get("Client", ""),
        "rawjson": row.get("RawJson") or row.get("rawJson") or "",
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
        lf.create_dataset(name=dataset_name, description=f"Superbill Checkpoint ({env})")
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
        lf.create_dataset(name=dataset_name, description=f"Superbill Predictions ({env})")
        for rec in records:
            lf.create_dataset_item(
                dataset_name=dataset_name,
                input={k: rec[k] for k in ["id", "filename", "client_name", "rawjson", "ground_truth", "total_matches", "total_mismatches", "accuracy_percentage"]},
                metadata={"record_id": rec["id"], "source": "Superbill", "environment": env}
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
# MAIN SUPERBILL PIPELINE
# ==========================================
def healthcare_superbill_push(ids_per_message: int = 5, max_messages: int = 10):
    env = os.getenv("LANGFUSE_ENVIRONMENT", "dev").strip()
    checkpoint_ds = f"healthcare_accuracy_superbill_{env}"
    predictions_ds = f"healthcare_predictions_superbill_{env}"
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
            "allocation": resolve_table_name(conn, ["AllocationRecord", "SuperBillAllocation", "SuperBillAllocations"]),
            "claim": resolve_table_name(conn, ["ClaimRecord", "SuperBillClaim", "SuperBillClaims"]),
            "payer": resolve_table_name(conn, ["PayerRecord", "SuperBillPayer", "SuperBillPayers"]),
            "provider": resolve_table_name(conn, ["ClaimProviderRecord", "SuperBillClaimProvider", "SuperBillClaimProviders"]),
            "patient": resolve_table_name(conn, ["PatientRecord", "PatientRecords"]),
            "dx": resolve_table_name(conn, ["DiagnosisCodesRecord", "SuperBillDiagnosisCode", "SuperBillDiagnosisCodes"]),
            "sl": resolve_table_name(conn, ["ServiceLineRecord", "SuperBillServiceLine", "SuperBillServiceLines"]),
        }

        last_id = _load_checkpoint(checkpoint_ds)
        query = f"SELECT Id, RawJson FROM `{tables['allocation']}` WHERE RawJson IS NOT NULL AND RawJson != ''"
        params = []
        if last_id:
            query += " AND Id > %s"
            params.append(last_id)
        query += " ORDER BY Id ASC"

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = [{"Id": r[0], "RawJson": r[1]} for r in cur.fetchall()]

        records = []
        for row in rows:
            eval_metrics = evaluate_superbill_record(row, conn, tables)
            records.append({"id": str(row["Id"]), **eval_metrics})

        if records:
            chunks = [records[i:i + ids_per_message] for i in range(0, len(records), ids_per_message)][:max_messages]
            msgs = [json.dumps({"record_ids": [r["id"] for r in chunk], "source": "healthcare_superbill"}) for chunk in chunks]
            
            _send_to_azure_queue(queue_name, msgs)
            _save_checkpoint(checkpoint_ds, env, chunks[-1][-1]["id"])
            if save_preds:
                _save_predictions(predictions_ds, env, [r for c in chunks for r in c])
            logging.info(f"Processed {len(chunks)} chunks of Superbill records.")

    finally:
        conn.close()

if __name__ == "__main__":
    healthcare_superbill_push()