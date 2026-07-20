# #!/usr/bin/env python3
# """
# Generate Superbill fine-tuning data.

# For each Superbill allocation that has a RawJson:
#   1. Parse the RawJson (AI's initial extraction).
#   2. Query all related DB tables to get user-corrected values.
#   3. Overlay the DB values onto the RawJson structure –> ground truth.
#   4. Optionally send output to Service Bus or save output parameters.

# Environment variables:
#   HEALTHCARE_AI_DB_SERVER / PORT / USERID / PASSWORD / DATABASE
#   or HEALTHCARE_AI_DB_JDBC_URL
#   SERVICE_BUS_CONNECTION_STRING / SERVICE_BUS_QUEUE_NAME
# """

# from __future__ import annotations

# import datetime as dt
# import json
# import os
# import re
# import sys
# import logging
# from urllib.parse import unquote, urlparse
# from dataclasses import dataclass
# from decimal import Decimal, InvalidOperation
# from typing import Any, Dict, List, Optional, Sequence, Tuple

# import pymysql
# from pymysql.cursors import DictCursor

# # Ensure basic logging setup is present if executing directly outside Azure Functions
# if not logging.getLogger().handlers:
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")

# # ---------------------------------------------------------------------------
# # Config helpers (adapted from EOB with default fallbacks)
# # ---------------------------------------------------------------------------

# USE_RAW_FALLBACK_WHEN_DB_MISSING = False
# DB_IN_CLAUSE_CHUNK_SIZE = 1000
# STRICT_AUDIT_MODE = True
# STRICT_AUDIT_FAIL_ON_MISMATCH = True
# DOWNLOAD_PDFS = False


# def env_bool(name: str) -> bool:
#     raw = os.getenv(name, "").strip().lower()
#     if raw in {"1", "true", "yes", "y", "on"}:
#         return True
#     if raw in {"0", "false", "no", "n", "off"}:
#         return False
#     raise ValueError(f"Invalid boolean value for {name}: {raw!r}")


# def env_int(name: str) -> int:
#     raw = os.getenv(name, "").strip()
#     try:
#         return int(raw)
#     except ValueError:
#         raise ValueError(f"Invalid integer value for {name}: {raw!r}")


# def apply_env_overrides() -> Dict[str, str]:
#     global USE_RAW_FALLBACK_WHEN_DB_MISSING
#     global DB_IN_CLAUSE_CHUNK_SIZE
#     global STRICT_AUDIT_MODE
#     global STRICT_AUDIT_FAIL_ON_MISMATCH
#     global DOWNLOAD_PDFS

#     download_pdfs_raw = os.getenv("SUPERBILL_DOWNLOAD_PDFS", "").strip()
#     if download_pdfs_raw:
#         try:
#             DOWNLOAD_PDFS = env_bool("SUPERBILL_DOWNLOAD_PDFS")
#         except ValueError:
#             logging.warning("Invalid boolean value for SUPERBILL_DOWNLOAD_PDFS, defaulting to False.")
#             DOWNLOAD_PDFS = False
#     else:
#         DOWNLOAD_PDFS = False

#     USE_RAW_FALLBACK_WHEN_DB_MISSING = False
#     raw_fallback = os.getenv("SUPERBILL_USE_RAW_FALLBACK_WHEN_DB_MISSING", "").strip()
#     if raw_fallback:
#         try:
#             USE_RAW_FALLBACK_WHEN_DB_MISSING = env_bool("SUPERBILL_USE_RAW_FALLBACK_WHEN_DB_MISSING")
#         except ValueError:
#             pass

#     DB_IN_CLAUSE_CHUNK_SIZE = 1000
#     chunk_size_raw = os.getenv("SUPERBILL_DB_IN_CLAUSE_CHUNK_SIZE", "").strip()
#     if chunk_size_raw:
#         try:
#             DB_IN_CLAUSE_CHUNK_SIZE = max(1, env_int("SUPERBILL_DB_IN_CLAUSE_CHUNK_SIZE"))
#         except ValueError:
#             pass

#     STRICT_AUDIT_MODE = True
#     audit_mode_raw = os.getenv("SUPERBILL_STRICT_AUDIT_MODE", "").strip()
#     if audit_mode_raw:
#         try:
#             STRICT_AUDIT_MODE = env_bool("SUPERBILL_STRICT_AUDIT_MODE")
#         except ValueError:
#             pass

#     STRICT_AUDIT_FAIL_ON_MISMATCH = True
#     audit_fail_raw = os.getenv("SUPERBILL_STRICT_AUDIT_FAIL_ON_MISMATCH", "").strip()
#     if audit_fail_raw:
#         try:
#             STRICT_AUDIT_FAIL_ON_MISMATCH = env_bool("SUPERBILL_STRICT_AUDIT_FAIL_ON_MISMATCH")
#         except ValueError:
#             pass

#     output_dir = os.getenv("SUPERBILL_OUTPUT_DIR", "SUPERBILL_Fine_Tuning_data").strip()
#     output_file = os.getenv("SUPERBILL_OUTPUT_FILE", "superbill_fine_tuning.json").strip()
#     pdf_output_subdir = os.getenv("SUPERBILL_PDF_OUTPUT_SUBDIR", "pdfs").strip()

#     if DOWNLOAD_PDFS:
#         missing_storage = []
#         if not os.getenv("AZURE_STORAGE_CONNECTION_STRING_HEALTHCARE_AI", "").strip():
#             missing_storage.append("AZURE_STORAGE_CONNECTION_STRING_HEALTHCARE_AI")
#         if not os.getenv("HEALTHCARE_AI_CONTAINER_NAME_SUPERBILL", "").strip():
#             missing_storage.append("HEALTHCARE_AI_CONTAINER_NAME_SUPERBILL")
#         if missing_storage:
#             raise ValueError(
#                 f"PDF download is enabled (SUPERBILL_DOWNLOAD_PDFS=True) but missing storage config: {', '.join(missing_storage)}"
#             )

#     return {
#         "output_dir": output_dir,
#         "output_file": output_file,
#         "pdf_output_subdir": pdf_output_subdir,
#     }


# def load_dotenv_file(path: str = ".env") -> None:
#     if not os.path.exists(path):
#         return
#     try:
#         with open(path, "r", encoding="utf-8") as fh:
#             for raw in fh:
#                 line = raw.strip()
#                 if not line or line.startswith("#") or "=" not in line:
#                     continue
#                 k, v = line.split("=", 1)
#                 k, v = k.strip(), v.strip()
#                 if not k:
#                     continue
#                 if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
#                     v = v[1:-1]
#                 os.environ.setdefault(k, v)
#     except OSError:
#         return


# @dataclass
# class DbConfig:
#     host: str
#     port: int
#     user: str
#     password: str
#     database: str


# def _parse_jdbc(jdbc_url: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
#     m = re.match(r"^jdbc:mysql://([^/:?#]+)(?::(\d+))?/([^?]+)", jdbc_url.strip(), re.IGNORECASE)
#     if not m:
#         return None, None, None
#     return m.group(1), int(m.group(2)) if m.group(2) else None, m.group(3)


# def resolve_db_config() -> DbConfig:
#     host = os.getenv("HEALTHCARE_AI_DB_SERVER")
#     port_raw = os.getenv("HEALTHCARE_AI_DB_PORT")
#     user = os.getenv("HEALTHCARE_AI_DB_USERID")
#     password = os.getenv("HEALTHCARE_AI_DB_PASSWORD")
#     database = os.getenv("HEALTHCARE_AI_DB_DATABASE")
#     jdbc = os.getenv("HEALTHCARE_AI_DB_JDBC_URL")
#     if jdbc:
#         j_host, j_port, j_db = _parse_jdbc(jdbc)
#         host = host or j_host
#         if port_raw is None and j_port is not None:
#             port_raw = str(j_port)
#         database = database or j_db
#     if not host or not user or not password or not database:
#         raise ValueError(
#             "Missing DB config. Set HEALTHCARE_AI_DB_SERVER/USERID/PASSWORD/DATABASE "
#             "or HEALTHCARE_AI_DB_JDBC_URL."
#         )
#     return DbConfig(host=host, port=int(port_raw) if port_raw else 3306,
#                      user=user, password=password, database=database)


# # ---------------------------------------------------------------------------
# # DB helpers
# # ---------------------------------------------------------------------------

# def q(name: str) -> str:
#     return f"`{name}`"


# def resolve_table_name(conn, candidates: Sequence[str]) -> str:
#     db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db
#     with conn.cursor(DictCursor) as cur:
#         cur.execute(
#             "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s",
#             (db_name,),
#         )
#         rows = cur.fetchall()
#     existing = {r["TABLE_NAME"].lower(): r["TABLE_NAME"] for r in rows}
#     for c in candidates:
#         hit = existing.get(c.lower())
#         if hit:
#             return hit
#     raise RuntimeError(f"None of table candidates exist: {candidates}")


# def fetch_allocations(conn, table: str, allocation_id: Optional[int],
#                       max_rows: int, fetch_all: bool) -> List[Dict]:
#     with conn.cursor(DictCursor) as cur:
#         if allocation_id is not None:
#             cur.execute(f"SELECT * FROM {q(table)} WHERE Id=%s", (allocation_id,))
#         else:
#             where = "(RawJson IS NOT NULL AND RawJson <> '') OR (rawJson IS NOT NULL AND rawJson <> '')"
#             if fetch_all:
#                 cur.execute(f"SELECT * FROM {q(table)} WHERE {where} ORDER BY Id asc")
#             else:
#                 cur.execute(
#                     f"SELECT * FROM {q(table)} WHERE {where} ORDER BY Id ASC LIMIT %s",
#                     (max_rows,),
#                 )
#         return cur.fetchall()


# def fetch_by_fk(conn, table: str, fk: str, fk_val: Any) -> List[Dict]:
#     with conn.cursor(DictCursor) as cur:
#         cur.execute(f"SELECT * FROM {q(table)} WHERE {q(fk)}=%s ORDER BY Id", (fk_val,))
#         return cur.fetchall()


# def fetch_by_fk_many(conn, table: str, fk: str, fk_vals: Sequence[Any]) -> List[Dict]:
#     values = list(dict.fromkeys(v for v in fk_vals if v is not None))
#     if not values:
#         return []
#     rows: List[Dict] = []
#     with conn.cursor(DictCursor) as cur:
#         for start in range(0, len(values), DB_IN_CLAUSE_CHUNK_SIZE):
#             chunk = values[start:start + DB_IN_CLAUSE_CHUNK_SIZE]
#             placeholders = ", ".join(["%s"] * len(chunk))
#             sql = f"SELECT * FROM {q(table)} WHERE {q(fk)} IN ({placeholders}) ORDER BY Id"
#             cur.execute(sql, tuple(chunk))
#             rows.extend(cur.fetchall())
#     return rows


# def fetch_by_ids(conn, table: str, row_ids: Sequence[Any]) -> List[Dict]:
#     values = list(dict.fromkeys(v for v in row_ids if v is not None))
#     if not values:
#         return []
#     rows: List[Dict] = []
#     with conn.cursor(DictCursor) as cur:
#         for start in range(0, len(values), DB_IN_CLAUSE_CHUNK_SIZE):
#             chunk = values[start:start + DB_IN_CLAUSE_CHUNK_SIZE]
#             placeholders = ", ".join(["%s"] * len(chunk))
#             sql = f"SELECT * FROM {q(table)} WHERE Id IN ({placeholders})"
#             cur.execute(sql, tuple(chunk))
#             rows.extend(cur.fetchall())
#     return rows


# def group_rows_by_key(rows: Sequence[Dict], key: str) -> Dict[Any, List[Dict]]:
#     grouped: Dict[Any, List[Dict]] = {}
#     for row in rows:
#         row_key = row.get(key)
#         if row_key is None:
#             continue
#         grouped.setdefault(row_key, []).append(row)
#     return grouped


# def map_rows_by_id(rows: Sequence[Dict]) -> Dict[Any, Dict]:
#     return {row.get("Id"): row for row in rows if row.get("Id") is not None}


# def sanitize_file_name(name: str) -> str:
#     cleaned = (name or "").strip() or "unknown_file.pdf"
#     cleaned = re.sub(r'[<>:"/\\|?*]+', "_", cleaned)
#     return cleaned


# def extract_blob_name(file_url: Any, file_name: Any, container_name: str) -> str:
#     file_url_text = (str(file_url).strip() if file_url is not None else "")
#     if file_url_text:
#         parsed = urlparse(file_url_text)
#         path = unquote(parsed.path.lstrip("/"))
#         if path:
#             marker = f"{container_name}/"
#             idx = path.lower().find(marker.lower())
#             if idx >= 0:
#                 return path[idx + len(marker):]
#             return path
#     return str(file_name).strip() if file_name is not None else ""


# def try_download_pdf_for_allocation(
#     container_client,
#     allocation_row: Dict,
#     target_dir: str,
#     container_name: str,
# ) -> str:
#     file_name = sanitize_file_name(str(allocation_row.get("File_name") or "unknown_file.pdf"))
#     if not file_name.lower().endswith(".pdf"):
#         file_name = f"{file_name}.pdf"

#     target_path = os.path.join(target_dir, file_name)
#     if os.path.exists(target_path):
#         return "skipped_existing"

#     blob_name = extract_blob_name(
#         allocation_row.get("File_url"),
#         allocation_row.get("File_name"),
#         container_name,
#     ).lstrip("/")
#     if not blob_name:
#         return "failed_missing_blob_name"

#     try:
#         blob_client = container_client.get_blob_client(blob_name)
#         with open(target_path, "wb") as fh:
#             fh.write(blob_client.download_blob().readall())
#         return "downloaded"
#     except Exception:
#         return "failed_download"


# # ---------------------------------------------------------------------------
# # Type formatting – DB values –> RawJson-compatible strings
# # ---------------------------------------------------------------------------

# def fmt_str(v: Any) -> Optional[str]:
#     """Return a cleaned string or None."""
#     if v is None:
#         return None
#     s = str(v).strip()
#     return s if s else None


# def fmt_money(v: Any) -> Optional[str]:
#     """Decimal / float / int –> string like '123.45'."""
#     if v is None:
#         return None
#     try:
#         d = Decimal(str(v))
#         return str(d)
#     except (InvalidOperation, ValueError):
#         return None


# def fmt_date(v: Any) -> Optional[str]:
#     """date/datetime –> MM-DD-YYYY string (matches common RawJson format)."""
#     if v is None:
#         return None
#     if isinstance(v, dt.datetime):
#         v = v.date()
#     if isinstance(v, dt.date):
#         return v.strftime("%m-%d-%Y")
#     return fmt_str(v)


# def fmt_int(v: Any) -> Optional[int]:
#     if v is None:
#         return None
#     try:
#         return int(v)
#     except (ValueError, TypeError):
#         return None


# FILE_STATUS_ENUM = {
#     0: "Pending", 1: "Failed", 2: "Completed",
#     3: "ManuallyCreated", 4: "PartiallyCompleted",
# }


# def fmt_status(v: Any) -> Optional[str]:
#     if v is None:
#         return None
#     if isinstance(v, int):
#         return FILE_STATUS_ENUM.get(v, str(v))
#     s = str(v).strip()
#     if s.isdigit():
#         return FILE_STATUS_ENUM.get(int(s), s)
#     return s


# # ---------------------------------------------------------------------------
# # Build helpers – wrap a DB value in {"value": "..."} to match RawJson shape
# # ---------------------------------------------------------------------------

# def ocr_node(value: Any) -> Optional[Dict[str, Any]]:
#     """Wrap a scalar into the OCR-style {'value': ...} node used in RawJson."""
#     if value is None:
#         return None
#     return {"value": value}


# def ocr_node_str(v: Any) -> Optional[Dict[str, Any]]:
#     s = fmt_str(v)
#     return ocr_node(s) if s is not None else None


# def ocr_node_money(v: Any) -> Optional[Dict[str, Any]]:
#     s = fmt_money(v)
#     return ocr_node(s) if s is not None else None


# def ocr_node_date(v: Any) -> Optional[Dict[str, Any]]:
#     s = fmt_date(v)
#     return ocr_node(s) if s is not None else None


# def ocr_node_int(v: Any) -> Optional[Dict[str, Any]]:
#     val = fmt_int(v)
#     return ocr_node(val) if val is not None else None


# def missing_section_value(original: Optional[Dict]) -> Optional[Dict]:
#     return original if USE_RAW_FALLBACK_WHEN_DB_MISSING else None


# # ---------------------------------------------------------------------------
# # Ground-truth builders – mapped specifically to Superbill
# # ---------------------------------------------------------------------------

# def build_gt_allocation(db_row: Dict, original_alloc: Dict) -> Dict:
#     gt = dict(original_alloc)

#     # Direct-copy/scalar fields for Allocation level (not wrapped in OCR value nodes)
#     gt["File_name"] = db_row.get("File_name")
#     gt["File_url"] = db_row.get("File_url")
#     gt["Client"] = db_row.get("Client")
#     gt["Account"] = db_row.get("Account")
#     gt["Total_Charge_Amt_On_File"] = fmt_money(db_row.get("Total_Charge_Amt_On_File"))
#     gt["Date_Of_Service"] = fmt_date(db_row.get("Date_Of_Service"))
#     gt["Download_Date"] = fmt_date(db_row.get("Download_Date"))
#     gt["Completed_Date"] = fmt_date(db_row.get("Completed_Date"))
#     gt["Not_Completed_Reason"] = db_row.get("Not_Completed_Reason")
#     gt["File_Status"] = fmt_status(db_row.get("File_Status"))

#     return gt


# def build_gt_patient(db_patient: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
#     if db_patient is None:
#         return missing_section_value(original)
#     gt: Dict[str, Any] = {}
#     gt["Patient_FN"]             = ocr_node_str(db_patient.get("Patient_FN"))
#     gt["Patient_LN"]             = ocr_node_str(db_patient.get("Patient_LN"))
#     gt["Patient_MI"]             = ocr_node_str(db_patient.get("Patient_MI"))
#     gt["Patient_Id"]             = ocr_node_str(db_patient.get("Patient_Id"))
#     gt["Patient_Account_Number"] = ocr_node_str(db_patient.get("Patient_Account_Number"))
#     gt["Patient_Control_Number"] = ocr_node_str(db_patient.get("Patient_Control_Number"))
#     gt["Patient_Group"]          = ocr_node_str(db_patient.get("Patient_Group"))
#     gt["Patient_Addr1"]          = ocr_node_str(db_patient.get("Patient_Addr1"))
#     gt["Patient_Addr2"]          = ocr_node_str(db_patient.get("Patient_Addr2"))
#     gt["Patient_City"]           = ocr_node_str(db_patient.get("Patient_City"))
#     gt["Patient_State"]          = ocr_node_str(db_patient.get("Patient_State"))
#     gt["Patient_Zip"]            = ocr_node_str(db_patient.get("Patient_Zip"))
#     gt["Patient_DOB"]            = ocr_node_date(db_patient.get("Patient_DOB"))
#     gt["Patient_Gender"]         = ocr_node_str(db_patient.get("Patient_Gender"))
#     gt["Patient_Relationship"]   = ocr_node_str(db_patient.get("Patient_Relationship"))
#     gt["Patient_Marital_Status"] = ocr_node_str(db_patient.get("Patient_Marital_Status"))
#     gt["Patient_Primary_Phone_No"] = ocr_node_str(db_patient.get("Patient_Primary_Phone_No"))
#     gt["Patient_Home_Phone_No"]    = ocr_node_str(db_patient.get("Patient_Home_Phone_No"))
#     gt["Patient_Primary_Email"]    = ocr_node_str(db_patient.get("Patient_Primary_Email"))
#     gt["Insured_Name"]           = ocr_node_str(db_patient.get("Insured_Name"))
#     return gt


# def build_gt_payer(db_payer: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
#     if db_payer is None:
#         return missing_section_value(original)
#     gt: Dict[str, Any] = {}
#     gt["Payer_Name"]  = ocr_node_str(db_payer.get("Payer_Name"))
#     gt["Payer_Id"]    = ocr_node_str(db_payer.get("Payer_Id"))
#     gt["Payer_Addr1"] = ocr_node_str(db_payer.get("Payer_Addr1"))
#     gt["Payer_Addr2"] = ocr_node_str(db_payer.get("Payer_Addr2"))
#     gt["Payer_City"]  = ocr_node_str(db_payer.get("Payer_City"))
#     gt["Payer_State"] = ocr_node_str(db_payer.get("Payer_State"))
#     gt["Payer_Zip"]   = ocr_node_str(db_payer.get("Payer_Zip"))
#     gt["Payer_Type"]  = ocr_node_str(db_payer.get("Payer_Type"))
#     return gt


# def build_gt_provider(db_provider: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
#     if db_provider is None:
#         return missing_section_value(original)
#     gt: Dict[str, Any] = {}
#     gt["Provider_Name"]     = ocr_node_str(db_provider.get("Provider_Name"))
#     gt["Provider_NPI"]      = ocr_node_str(db_provider.get("Provider_NPI"))
#     gt["Provider_Addr1"]    = ocr_node_str(db_provider.get("Provider_Addr1"))
#     gt["Provider_Addr2"]    = ocr_node_str(db_provider.get("Provider_Addr2"))
#     gt["Provider_City"]     = ocr_node_str(db_provider.get("Provider_City"))
#     gt["Provider_State"]    = ocr_node_str(db_provider.get("Provider_State"))
#     gt["Provider_Zip"]      = ocr_node_str(db_provider.get("Provider_Zip"))
#     gt["Provider_FedId"]    = ocr_node_str(db_provider.get("Provider_FedId"))
#     gt["Provider_TaxId"]    = ocr_node_str(db_provider.get("Provider_TaxId"))
#     gt["Provider_Taxonomy"] = ocr_node_str(db_provider.get("Provider_Taxonomy"))
#     return gt


# def build_gt_facility(db_provider: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
#     if db_provider is None:
#         return missing_section_value(original)
#     gt: Dict[str, Any] = {}
#     gt["Facility_Name"]     = ocr_node_str(db_provider.get("Provider_Name"))
#     gt["Facility_NPI"]      = ocr_node_str(db_provider.get("Provider_NPI"))
#     gt["Facility_Addr1"]    = ocr_node_str(db_provider.get("Provider_Addr1"))
#     gt["Facility_Addr2"]    = ocr_node_str(db_provider.get("Provider_Addr2"))
#     gt["Provider_City"]     = ocr_node_str(db_provider.get("Provider_City"))
#     gt["Facility_State"]    = ocr_node_str(db_provider.get("Provider_State"))
#     gt["Facility_Zip"]      = ocr_node_str(db_provider.get("Provider_Zip"))
#     gt["Facility_FedId"]    = ocr_node_str(db_provider.get("Provider_FedId"))
#     gt["Facility_TaxId"]    = ocr_node_str(db_provider.get("Provider_TaxId"))
#     gt["Facility_Taxonomy"] = ocr_node_str(db_provider.get("Provider_Taxonomy"))
#     return gt


# def build_gt_diagnosis(db_dx_rows: List[Dict], original: Optional[Dict]) -> Optional[Dict]:
#     if not db_dx_rows:
#         return missing_section_value(original)
#     dx = db_dx_rows[0]
#     gt: Dict[str, Any] = {}
#     gt["Primary_DX"] = ocr_node_str(dx.get("Primary_DX"))
#     for i in range(1, 13):
#         key = f"Secondary_DX{i}"
#         gt[key] = ocr_node_str(dx.get(key))
#     return gt


# def build_gt_service_line(db_sl: Dict) -> Dict:
#     gt: Dict[str, Any] = {}
#     gt["Service_From_Date"]  = ocr_node_date(db_sl.get("Service_From_Date"))
#     gt["Service_To_Date"]    = ocr_node_date(db_sl.get("Service_To_Date"))
#     gt["Procedure_Code"]     = ocr_node_str(db_sl.get("Procedure_Code"))
#     gt["Mod1"]               = ocr_node_str(db_sl.get("Mod1"))
#     gt["Mod2"]               = ocr_node_str(db_sl.get("Mod2"))
#     gt["Mod3"]               = ocr_node_str(db_sl.get("Mod3"))
#     gt["Mod4"]               = ocr_node_str(db_sl.get("Mod4"))
#     gt["Service_Billed_Amt"] = ocr_node_money(db_sl.get("Service_Billed_Amt"))
#     gt["D_U"]                = ocr_node_int(db_sl.get("D_U"))
#     gt["Place_Of_Service"]   = ocr_node_str(db_sl.get("Place_Of_Service"))
#     gt["DX_1"]               = ocr_node_str(db_sl.get("DX_1"))
#     gt["DX_2"]               = ocr_node_str(db_sl.get("DX_2"))
#     gt["DX_3"]               = ocr_node_str(db_sl.get("DX_3"))
#     gt["DX_4"]               = ocr_node_str(db_sl.get("DX_4"))
#     gt["User_Status"]        = ocr_node_str(db_sl.get("User_Status"))
#     return gt


# def build_gt_claim(
#     db_claim: Dict,
#     original_claim: Optional[Dict],
#     payer_rows: List[Dict],
#     provider_rows: List[Dict],
#     patient_by_id: Dict[Any, Dict],
#     dx_by_claim_id: Dict[Any, List[Dict]],
#     service_lines_by_claim_id: Dict[Any, List[Dict]],
# ) -> Dict:
#     """Build a single Superbill Claim ground-truth dict from DB rows."""
#     gt: Dict[str, Any] = {}
#     if original_claim:
#         gt.update(original_claim)

#     # --- Claim-level OCR value fields ---
#     gt["Patient_Control_Number"] = ocr_node_str(db_claim.get("Patient_Control_Number"))
#     gt["Claim_Total_Charge_Amt"] = ocr_node_money(db_claim.get("Claim_Total_Charge_Amt"))
#     gt["Claim_Filing_indicator"] = ocr_node_str(db_claim.get("Claim_Filing_indicator"))
#     gt["Claim_Frequency_Code"]   = ocr_node_str(db_claim.get("Claim_Frequency_Code"))
#     gt["Claim_Date_of_Service"]  = ocr_node_date(db_claim.get("Claim_Date_of_Service"))
#     gt["Claim_Auth_No"]          = ocr_node_str(db_claim.get("Claim_Auth_No"))
#     gt["Patient_Paid_Amt"]       = ocr_node_money(db_claim.get("Patient_Paid_Amt"))
#     gt["CLIA_Number"]            = ocr_node_str(db_claim.get("CLIA_Number"))
#     gt["CLaim_Invoice_Number"]   = ocr_node_str(db_claim.get("CLaim_Invoice_Number"))
#     gt["Admission_Date"]         = ocr_node_date(db_claim.get("Admission_Date"))
#     gt["Patient_Last_Seen_Date"] = ocr_node_date(db_claim.get("Patient_Last_Seen_Date"))

#     # --- Sub-entities: Patient ---
#     gt["Patient"] = build_gt_patient(
#         patient_by_id.get(db_claim.get("PatientId")),
#         original_claim.get("Patient") if original_claim else None,
#     )

#     # --- Sub-entities: Payer (List) ---
#     original_payers = (original_claim.get("Payer") or []) if original_claim else []
#     gt_payers = []
#     for idx, db_payer in enumerate(payer_rows):
#         orig_p = original_payers[idx] if idx < len(original_payers) else None
#         gt_payers.append(build_gt_payer(db_payer, orig_p))
#     gt["Payer"] = gt_payers

#     # --- Sub-entities: Providers (By Role Key Mapping) ---
#     # Map roles based on provider roles definition
#     role_to_provider = {prov.get("Role"): prov for prov in provider_rows if prov.get("Role")}
    
#     gt["BillingProvider"] = build_gt_provider(
#         role_to_provider.get("BillingProvider"),
#         original_claim.get("BillingProvider") if original_claim else None
#     )
#     gt["ServicingProvider"] = build_gt_provider(
#         role_to_provider.get("ServicingProvider"),
#         original_claim.get("ServicingProvider") if original_claim else None
#     )
#     gt["ReferringProvider"] = build_gt_provider(
#         role_to_provider.get("ReferringProvider"),
#         original_claim.get("ReferringProvider") if original_claim else None
#     )
#     gt["OrderingProvider"] = build_gt_provider(
#         role_to_provider.get("OrderingProvider"),
#         original_claim.get("OrderingProvider") if original_claim else None
#     )
#     gt["SupervisingProvider"] = build_gt_provider(
#         role_to_provider.get("SupervisingProvider"),
#         original_claim.get("SupervisingProvider") if original_claim else None
#     )
#     gt["ServicingFacility"] = build_gt_facility(
#         role_to_provider.get("ServiceFacilityProvider"),
#         original_claim.get("ServicingFacility") if original_claim else None
#     )

#     # --- Diagnosis ---
#     claim_id = db_claim.get("Id")
#     db_dx = dx_by_claim_id.get(claim_id, [])
#     gt["ClaimDiagnosisCodes"] = build_gt_diagnosis(
#         db_dx,
#         original_claim.get("ClaimDiagnosisCodes") if original_claim else None,
#     )

#     # --- Service Lines ---
#     db_sls = service_lines_by_claim_id.get(claim_id, [])
#     gt_sls = []
#     for db_sl in db_sls:
#         gt_sls.append(build_gt_service_line(db_sl))
#     gt["ServiceLines"] = gt_sls

#     return gt


# # ---------------------------------------------------------------------------
# # JSON serializer for Decimal / date / datetime
# # ---------------------------------------------------------------------------

# class _Encoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, Decimal):
#             return str(obj)
#         if isinstance(obj, (dt.date, dt.datetime)):
#             return obj.strftime("%m-%d-%Y")
#         if isinstance(obj, bytes):
#             return obj.decode("utf-8", errors="replace")
#         return super().default(obj)


# def dated_json_filename(filename: str) -> str:
#     stem, ext = os.path.splitext(filename)
#     if not ext:
#         ext = ".json"
#         stem = filename
#     run_date = dt.datetime.now().strftime("%Y%m%d")
#     return f"{stem}_{run_date}{ext}"


# # ---------------------------------------------------------------------------
# # Service Bus Queue Dispatch Helper
# # ---------------------------------------------------------------------------

# def _send_superbill_records_to_queue(
#     records: List[Dict[str, Any]],
#     ids_per_message: int,
#     max_messages_per_run: Optional[int],
#     progress_log_batch_size: int = 10,
# ) -> int:
#     """Send Superbill fine-tuning records to Service Bus queue in chunked messages."""
#     queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
#     conn_string = os.getenv("SERVICE_BUS_CONNECTION_STRING", "").strip()
    
#     if not queue_name or not conn_string:
#         logging.warning("[Queue] Skipping: SERVICE_BUS_QUEUE_NAME or SERVICE_BUS_CONNECTION_STRING not set")
#         return 0
    
#     if not records:
#         logging.info("[Queue] No records to send")
#         return 0
    
#     try:
#         from azure.servicebus import ServiceBusClient, ServiceBusMessage
        
#         client = ServiceBusClient.from_connection_string(conn_string)
#         messages_sent = 0
#         records_dispatched = 0

#         chunks = [
#             records[i:i + ids_per_message]
#             for i in range(0, len(records), ids_per_message)
#         ]
#         if max_messages_per_run is not None and max_messages_per_run > 0:
#             chunks = chunks[:max_messages_per_run]
        
#         with client.get_queue_sender(queue_name, socket_timeout=10.0) as sender:
#             for i, chunk in enumerate(chunks):
#                 allocation_ids = [rec.get("allocation_id") for rec in chunk]
#                 chunk_payload = [
#                     {
#                         "file_name": rec.get("file_name"),
#                         "allocation_id": rec.get("allocation_id"),
#                         "ground_truth": rec.get("ground_truth"),
#                     }
#                     for rec in chunk
#                 ]

#                 try:
#                     body = {
#                         "file_name": chunk_payload[0].get("file_name") if len(chunk_payload) == 1 else None,
#                         "allocation_id": chunk_payload[0].get("allocation_id") if len(chunk_payload) == 1 else None,
#                         "ground_truth": chunk_payload[0].get("ground_truth") if len(chunk_payload) == 1 else None,
#                         "allocation_ids": allocation_ids,
#                         "records": chunk_payload,
#                         "source": "healthcare_superbill",
#                         "environment": os.getenv("LANGFUSE_ENVIRONMENT", "exp"),
#                         "process_type": "FineTuning",
#                         "queued_at": dt.datetime.now(dt.timezone.utc).isoformat(),
#                     }
#                     msg = ServiceBusMessage(json.dumps(body, cls=_Encoder))
#                     sender.send_messages(msg)
#                     messages_sent += 1
#                     records_dispatched += len(chunk)
                    
#                     if (i + 1) % progress_log_batch_size == 0:
#                         logging.info(f"[Queue] Sent {messages_sent}/{len(chunks)} messages; records dispatched={records_dispatched}")
#                 except Exception as e:
#                     logging.warning(f"[Queue] Failed to send chunk with allocation_ids={allocation_ids}: {e}")
            
#             logging.info(f"[Queue] SUCCESS: {messages_sent}/{len(chunks)} messages sent; records dispatched={records_dispatched}")
#             return messages_sent
#     except Exception as e:
#         logging.error(f"[Queue] Error sending to queue: {e}")
#         return 0


# def superbill_fine_tuning_data_push(
#     ids_per_message: Optional[int] = None,
#     max_messages_per_run: Optional[int] = None
# ) -> None:
#     """
#     Generate Superbill fine-tuning data.
#     Callable by Azure Functions triggers as well as manual standalone executions.
#     """
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     load_dotenv_file(os.path.join(script_dir, ".env"))
#     try:
#         runtime_cfg = apply_env_overrides()
#     except Exception as ex:
#         logging.error(f"Config error: {ex}")
#         raise ex

#     outdir_name = runtime_cfg["output_dir"]
#     pdf_subdir = runtime_cfg["pdf_output_subdir"]

#     env_ids_per_message = (os.getenv("IDS_PER_MESSAGE") or "").strip()
#     eff_ids_per_message = ids_per_message if ids_per_message is not None else int(env_ids_per_message or "1")
#     if eff_ids_per_message <= 0:
#         raise ValueError("ids_per_message must be greater than 0")

#     env_max_messages = (os.getenv("MAX_MESSAGES_PER_RUN") or "").strip()
#     eff_max_messages_per_run = (
#         max_messages_per_run
#         if max_messages_per_run is not None
#         else int(env_max_messages) if env_max_messages else None
#     )
#     if eff_max_messages_per_run is not None and eff_max_messages_per_run <= 0:
#         raise ValueError("max_messages_per_run must be greater than 0 when provided")

#     logging.info(
#         "[Config] Effective queue batching: ids_per_message=%s, max_messages_per_run=%s",
#         eff_ids_per_message,
#         eff_max_messages_per_run,
#     )

#     outdir = outdir_name if os.path.isabs(outdir_name) else os.path.join(script_dir, outdir_name)
#     pdf_output_dir = os.path.join(outdir, pdf_subdir)
#     download_enabled = False
#     container_client = None

#     if DOWNLOAD_PDFS:
#         try:
#             from azure.storage.blob import BlobServiceClient
#             connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING_HEALTHCARE_AI", "").strip()
#             container_name = os.getenv("HEALTHCARE_AI_CONTAINER_NAME_SUPERBILL", "").strip()
#             os.makedirs(pdf_output_dir, exist_ok=True)
#             blob_service = BlobServiceClient.from_connection_string(connection_string)
#             container_client = blob_service.get_container_client(container_name)
#             download_enabled = True
#         except Exception as ex:
#             logging.error(f"Config error: could not initialize Azure Blob client: {ex}")
#             raise ex

#     try:
#         cfg = resolve_db_config()
#     except Exception as ex:
#         logging.error(f"Config error: {ex}")
#         raise ex

#     try:
#         conn = pymysql.connect(
#             host=cfg.host, port=cfg.port,
#             user=cfg.user, password=cfg.password,
#             database=cfg.database,
#             cursorclass=DictCursor, autocommit=True,
#         )
#     except Exception as ex:
#         logging.error(f"DB connection error: {ex}")
#         raise ex

#     with conn:
#         try:
#             tables = {
#                 "allocation": resolve_table_name(conn, ["AllocationRecord", "SuperBillAllocation", "SuperBillAllocations"]),
#                 "claim":      resolve_table_name(conn, ["ClaimRecord", "SuperBillClaim", "SuperBillClaims"]),
#                 "payer":      resolve_table_name(conn, ["PayerRecord", "SuperBillPayer", "SuperBillPayers"]),
#                 "provider":   resolve_table_name(conn, ["ClaimProviderRecord", "SuperBillClaimProvider", "SuperBillClaimProviders"]),
#                 "patient":    resolve_table_name(conn, ["PatientRecord", "PatientRecords"]),
#                 "dx":         resolve_table_name(conn, ["DiagnosisCodesRecord", "SuperBillDiagnosisCode", "SuperBillDiagnosisCodes"]),
#                 "sl":         resolve_table_name(conn, ["ServiceLineRecord", "SuperBillServiceLine", "SuperBillServiceLines"]),
#             }
#         except Exception as ex:
#             logging.error(f"Table resolution error: {ex}")
#             raise ex

#         fetch_all = eff_max_messages_per_run is None
#         max_rows = (eff_ids_per_message * eff_max_messages_per_run) if eff_max_messages_per_run is not None else 10

#         allocations = fetch_allocations(conn, tables["allocation"], None, max_rows, fetch_all)
#         if not allocations:
#             logging.info("No allocations found with RawJson.")
#             return

#         written = 0
#         skipped = 0
#         audit_mismatches = 0
#         downloaded_pdfs = 0
#         skipped_pdfs = 0
#         failed_pdfs = 0
#         outputs: List[Dict[str, Any]] = []

#         for alloc_row in sorted(allocations, key=lambda r: r.get("Id") or 0, reverse=True):
#             alloc_id = alloc_row.get("Id")

#             if download_enabled and container_client is not None:
#                 download_status = try_download_pdf_for_allocation(
#                     container_client,
#                     alloc_row,
#                     pdf_output_dir,
#                     container_name,
#                 )
#                 if download_status == "downloaded":
#                     downloaded_pdfs += 1
#                 elif download_status == "skipped_existing":
#                     skipped_pdfs += 1
#                 else:
#                     failed_pdfs += 1

#             file_name = alloc_row.get("File_name") or f"allocation_{alloc_id}"

#             raw_json_str = alloc_row.get("RawJson") or alloc_row.get("rawJson")
#             if not raw_json_str:
#                 logging.info(f"  SKIP AllocationId={alloc_id}: empty RawJson")
#                 skipped += 1
#                 continue

#             try:
#                 root = json.loads(raw_json_str)
#             except json.JSONDecodeError as ex:
#                 logging.info(f"  SKIP AllocationId={alloc_id}: RawJson parse error – {ex}")
#                 skipped += 1
#                 continue

#             original_alloc = root.get("Allocation")
#             if not isinstance(original_alloc, dict):
#                 logging.info(f"  SKIP AllocationId={alloc_id}: RawJson missing Allocation object")
#                 skipped += 1
#                 continue

#             # --- Build ground-truth Allocation ---
#             gt_alloc = build_gt_allocation(alloc_row, original_alloc)

#             # --- Build ground-truth Claims ---
#             original_claims = []
#             for ci in original_alloc.get("Claim_Info") or []:
#                 if isinstance(ci, dict) and isinstance(ci.get("Claim"), dict):
#                     original_claims.append(ci["Claim"])

#             db_claims = fetch_by_fk(conn, tables["claim"], "AllocationRecordId", alloc_id)
#             claim_ids = [c.get("Id") for c in db_claims if c.get("Id") is not None]

#             patient_ids = {c.get("PatientId") for c in db_claims if c.get("PatientId") is not None}
#             patient_by_id = map_rows_by_id(fetch_by_ids(conn, tables["patient"], list(patient_ids)))

#             # Fetch payees, payers, providers by foreign keys
#             payers_by_claim = group_rows_by_key(
#                 fetch_by_fk_many(conn, tables["payer"], "ClaimRecordId", claim_ids),
#                 "ClaimRecordId",
#             )
#             providers_by_claim = group_rows_by_key(
#                 fetch_by_fk_many(conn, tables["provider"], "ClaimRecordId", claim_ids),
#                 "ClaimRecordId",
#             )
#             dx_by_claim_id = group_rows_by_key(
#                 fetch_by_fk_many(conn, tables["dx"], "ClaimRecordId", claim_ids),
#                 "ClaimRecordId",
#             )
#             service_lines = fetch_by_fk_many(conn, tables["sl"], "ClaimRecordId", claim_ids)
#             service_lines_by_claim_id = group_rows_by_key(service_lines, "ClaimRecordId")

#             gt_claims_info = []
#             for idx, db_claim in enumerate(db_claims):
#                 orig = original_claims[idx] if idx < len(original_claims) else None
#                 claim_id = db_claim.get("Id")
                
#                 gt_claim = build_gt_claim(
#                     db_claim,
#                     orig,
#                     payers_by_claim.get(claim_id, []),
#                     providers_by_claim.get(claim_id, []),
#                     patient_by_id,
#                     dx_by_claim_id,
#                     service_lines_by_claim_id,
#                 )
#                 gt_claims_info.append({"Claim": gt_claim})

#             gt_alloc["Claim_Info"] = gt_claims_info

#             db_claim_count = len(db_claims)
#             out_claim_count = len(gt_claims_info)
#             db_service_line_count = len(service_lines)
#             out_service_line_count = sum(
#                 len((claim_info.get("Claim") or {}).get("ServiceLines") or [])
#                 for claim_info in gt_claims_info
#             )
#             has_mismatch = (
#                 db_claim_count != out_claim_count
#                 or db_service_line_count != out_service_line_count
#             )
#             if STRICT_AUDIT_MODE:
#                 status = "MISMATCH" if has_mismatch else "OK"
#                 logging.info(
#                     f"  AUDIT AllocationId={alloc_id}: "
#                     f"claims db={db_claim_count}, out={out_claim_count}; "
#                     f"service_lines db={db_service_line_count}, out={out_service_line_count} [{status}]"
#                 )
#             if has_mismatch:
#                 audit_mismatches += 1

#             # --- Assemble output ---
#             output = {
#                 "file_name": file_name,
#                 "allocation_id": alloc_id,
#                 "raw_json": root,
#                 "ground_truth": {"Allocation": gt_alloc},
#             }

#             outputs.append(output)
#             written += 1
#             logging.info(f"  OK AllocationId={alloc_id}")

#         outputs.sort(key=lambda item: item.get("allocation_id") or 0, reverse=True)
        
#         # Send records to Service Bus queue for downstream processing
#         if outputs:
#             queue_count = _send_superbill_records_to_queue(
#                 outputs,
#                 ids_per_message=eff_ids_per_message,
#                 max_messages_per_run=eff_max_messages_per_run,
#             )
#         else:
#             queue_count = 0

#     logging.info(f"Done. Written: {written}, Skipped: {skipped}")
#     if download_enabled:
#         logging.info(f"PDFs: downloaded={downloaded_pdfs}, skipped_existing={skipped_pdfs}, failed={failed_pdfs}")
#         logging.info(f"PDF folder: {pdf_output_dir}")
#     if STRICT_AUDIT_MODE:
#         logging.info(f"Audit mismatches: {audit_mismatches}")
#     if STRICT_AUDIT_MODE and STRICT_AUDIT_FAIL_ON_MISMATCH and audit_mismatches > 0:
#         raise ValueError("Strict audit failed due to mismatch in claim/service-line counts.")
#     logging.info(f"Queued: {queue_count} records to Service Bus")


# if __name__ == "__main__":
#     logging.info("Running Superbill Fine Tuning manually...")
#     try:
#         superbill_fine_tuning_data_push()
#         sys.exit(0)
#     except Exception as main_err:
#         logging.critical(f"FATAL: {main_err}", exc_info=True)
#         sys.exit(1)



#!/usr/bin/env python3
"""
Generate Superbill fine-tuning data with ClickHouse checkpointing.

For each Superbill allocation that has a RawJson:
    1. Load the last processed allocation ID from ClickHouse.
  2. Query DB for allocations > last_checkpoint_id.
  3. Parse the RawJson (AI's initial extraction).
  4. Query all related DB tables to get user-corrected values.
  5. Overlay the DB values onto the RawJson structure –> ground truth.
  6. Dispatch JSON structures to Service Bus.
    7. Save the new highest allocation ID back to ClickHouse.

Environment variables:
  HEALTHCARE_AI_DB_SERVER / PORT / USERID / PASSWORD / DATABASE
  or HEALTHCARE_AI_DB_JDBC_URL
  SERVICE_BUS_CONNECTION_STRING / SERVICE_BUS_QUEUE_NAME
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import logging
import time
from urllib.parse import unquote, urlparse
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
from pymysql.cursors import DictCursor

from clickhouse_store import SUPERBILL_FINETUNING_CHECKPOINT_TABLE, get_environment, load_checkpoint_int, save_checkpoint_int

# Ensure basic logging setup is present if executing directly outside Azure Functions
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")

# ---------------------------------------------------------------------------
# ClickHouse Checkpoint Utilities
# ---------------------------------------------------------------------------


def _load_checkpoint_from_langfuse(checkpoint_dataset_name: str) -> Optional[int]:
    """Retrieve last processed allocation_id from ClickHouse using folder-specific table."""
    logging.info(f"[ClickHouse] Loading checkpoint from table '{checkpoint_dataset_name}'...")
    try:
        last_id = load_checkpoint_int(checkpoint_dataset_name, get_environment())
        if last_id is None:
            logging.info("[ClickHouse] Checkpoint table empty. Clean start.")
            return None
        logging.info(f"[ClickHouse] SUCCESS: Retrieved checkpoint last_allocation_id='{last_id}'")
        return last_id
    except Exception as ex:
        logging.warning(f"[ClickHouse] Could not retrieve checkpoint: {ex}. Starting clean.")
    return None


def _save_checkpoint_to_langfuse(checkpoint_dataset_name: str, last_allocation_id: int) -> None:
    """Save last processed allocation_id to ClickHouse using folder-specific table."""
    logging.info(f"[ClickHouse] Saving checkpoint: last_allocation_id='{last_allocation_id}'...")
    try:
        save_checkpoint_int(checkpoint_dataset_name, get_environment(), last_allocation_id)
        logging.info(f"[ClickHouse] SUCCESS: Checkpoint saved with last_allocation_id='{last_allocation_id}'")
    except Exception as ex:
        logging.error(f"[ClickHouse] Checkpoint save failed: {ex}")


def _get_oldest_date_from_db(db_config: 'DbConfig') -> Optional[str]:
    """Retrieve the oldest date from Superbill database for records with RawJson."""
    logging.info("[Database] Fetching oldest date from database...")
    try:
        conn = pymysql.connect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            database=db_config.database,
            connect_timeout=30,
            charset="utf8mb4"
        )
        cursor = conn.cursor(DictCursor)
        
        # Query for oldest CreatedDate where RawJson exists
        query = (
            "SELECT MIN(DATE(CreatedDate)) as oldest_date FROM Allocations "
            "WHERE RawJson IS NOT NULL AND RawJson != '' "
            "LIMIT 1"
        )
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result.get('oldest_date'):
            oldest_date_str = f"{result['oldest_date']} 00:00:00"
            logging.info(f"[Database] SUCCESS: Oldest date found: {oldest_date_str}")
            return oldest_date_str
        else:
            logging.warning("[Database] No records found with RawJson")
            return None
    except Exception as e:
        logging.error(f"[Database] Failed to fetch oldest date: {e}", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Config helpers (adapted from EOB with default fallbacks)
# ---------------------------------------------------------------------------

USE_RAW_FALLBACK_WHEN_DB_MISSING = False
DB_IN_CLAUSE_CHUNK_SIZE = 1000
STRICT_AUDIT_MODE = True
STRICT_AUDIT_FAIL_ON_MISMATCH = True
DOWNLOAD_PDFS = False


def env_bool(name: str) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {raw!r}")


def env_int(name: str) -> int:
    raw = os.getenv(name, "").strip()
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"Invalid integer value for {name}: {raw!r}")


def apply_env_overrides() -> Dict[str, str]:
    global USE_RAW_FALLBACK_WHEN_DB_MISSING
    global DB_IN_CLAUSE_CHUNK_SIZE
    global STRICT_AUDIT_MODE
    global STRICT_AUDIT_FAIL_ON_MISMATCH
    global DOWNLOAD_PDFS

    download_pdfs_raw = os.getenv("SUPERBILL_DOWNLOAD_PDFS", "").strip()
    if download_pdfs_raw:
        try:
            DOWNLOAD_PDFS = env_bool("SUPERBILL_DOWNLOAD_PDFS")
        except ValueError:
            logging.warning("Invalid boolean value for SUPERBILL_DOWNLOAD_PDFS, defaulting to False.")
            DOWNLOAD_PDFS = False
    else:
        DOWNLOAD_PDFS = False

    USE_RAW_FALLBACK_WHEN_DB_MISSING = False
    raw_fallback = os.getenv("SUPERBILL_USE_RAW_FALLBACK_WHEN_DB_MISSING", "").strip()
    if raw_fallback:
        try:
            USE_RAW_FALLBACK_WHEN_DB_MISSING = env_bool("SUPERBILL_USE_RAW_FALLBACK_WHEN_DB_MISSING")
        except ValueError:
            pass

    DB_IN_CLAUSE_CHUNK_SIZE = 1000
    chunk_size_raw = os.getenv("SUPERBILL_DB_IN_CLAUSE_CHUNK_SIZE", "").strip()
    if chunk_size_raw:
        try:
            DB_IN_CLAUSE_CHUNK_SIZE = max(1, env_int("SUPERBILL_DB_IN_CLAUSE_CHUNK_SIZE"))
        except ValueError:
            pass

    STRICT_AUDIT_MODE = True
    audit_mode_raw = os.getenv("SUPERBILL_STRICT_AUDIT_MODE", "").strip()
    if audit_mode_raw:
        try:
            STRICT_AUDIT_MODE = env_bool("SUPERBILL_STRICT_AUDIT_MODE")
        except ValueError:
            pass

    STRICT_AUDIT_FAIL_ON_MISMATCH = True
    audit_fail_raw = os.getenv("SUPERBILL_STRICT_AUDIT_FAIL_ON_MISMATCH", "").strip()
    if audit_fail_raw:
        try:
            STRICT_AUDIT_FAIL_ON_MISMATCH = env_bool("SUPERBILL_STRICT_AUDIT_FAIL_ON_MISMATCH")
        except ValueError:
            pass

    output_dir = os.getenv("SUPERBILL_OUTPUT_DIR", "SUPERBILL_Fine_Tuning_data").strip()
    output_file = os.getenv("SUPERBILL_OUTPUT_FILE", "superbill_fine_tuning.json").strip()
    pdf_output_subdir = os.getenv("SUPERBILL_PDF_OUTPUT_SUBDIR", "pdfs").strip()

    if DOWNLOAD_PDFS:
        missing_storage = []
        if not os.getenv("AZURE_STORAGE_CONNECTION_STRING_HEALTHCARE_AI", "").strip():
            missing_storage.append("AZURE_STORAGE_CONNECTION_STRING_HEALTHCARE_AI")
        if not os.getenv("HEALTHCARE_AI_CONTAINER_NAME_SUPERBILL", "").strip():
            missing_storage.append("HEALTHCARE_AI_CONTAINER_NAME_SUPERBILL")
        if missing_storage:
            raise ValueError(
                f"PDF download is enabled (SUPERBILL_DOWNLOAD_PDFS=True) but missing storage config: {', '.join(missing_storage)}"
            )

    return {
        "output_dir": output_dir,
        "output_file": output_file,
        "pdf_output_subdir": pdf_output_subdir,
    }


def load_dotenv_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if not k:
                    continue
                if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                    v = v[1:-1]
                os.environ.setdefault(k, v)
    except OSError:
        return


@dataclass
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


def _parse_jdbc(jdbc_url: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    m = re.match(r"^jdbc:mysql://([^/:?#]+)(?::(\d+))?/([^?]+)", jdbc_url.strip(), re.IGNORECASE)
    if not m:
        return None, None, None
    return m.group(1), int(m.group(2)) if m.group(2) else None, m.group(3)


def resolve_db_config() -> DbConfig:
    host = os.getenv("HEALTHCARE_AI_DB_SERVER")
    port_raw = os.getenv("HEALTHCARE_AI_DB_PORT")
    user = os.getenv("HEALTHCARE_AI_DB_USERID")
    password = os.getenv("HEALTHCARE_AI_DB_PASSWORD")
    database = os.getenv("HEALTHCARE_AI_DB_DATABASE")
    jdbc = os.getenv("HEALTHCARE_AI_DB_JDBC_URL")
    if jdbc:
        j_host, j_port, j_db = _parse_jdbc(jdbc)
        host = host or j_host
        if port_raw is None and j_port is not None:
            port_raw = str(j_port)
        database = database or j_db
    if not host or not user or not password or not database:
        raise ValueError(
            "Missing DB config. Set HEALTHCARE_AI_DB_SERVER/USERID/PASSWORD/DATABASE "
            "or HEALTHCARE_AI_DB_JDBC_URL."
        )
    return DbConfig(host=host, port=int(port_raw) if port_raw else 3306,
                     user=user, password=password, database=database)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def q(name: str) -> str:
    return f"`{name}`"


def resolve_table_name(conn, candidates: Sequence[str]) -> str:
    db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db
    with conn.cursor(DictCursor) as cur:
        cur.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s",
            (db_name,),
        )
        rows = cur.fetchall()
    existing = {r["TABLE_NAME"].lower(): r["TABLE_NAME"] for r in rows}
    for c in candidates:
        hit = existing.get(c.lower())
        if hit:
            return hit
    raise RuntimeError(f"None of table candidates exist: {candidates}")


def fetch_allocations(conn, table: str, last_checkpoint_id: Optional[int], max_rows: int, fetch_all: bool) -> List[Dict]:
    """Fetch allocations ordered ASCENDING, filtering by checkpoint."""
    with conn.cursor(DictCursor) as cur:
        where_clauses = ["((RawJson IS NOT NULL AND RawJson <> '') OR (rawJson IS NOT NULL AND rawJson <> ''))"]
        query_params = []
        
        if last_checkpoint_id is not None:
            where_clauses.append("Id > %s")
            query_params.append(last_checkpoint_id)
            
        where = " AND ".join(where_clauses)
        
        if fetch_all:
            cur.execute(f"SELECT * FROM {q(table)} WHERE {where} ORDER BY Id ASC", tuple(query_params))
        else:
            query_params.append(max_rows)
            cur.execute(f"SELECT * FROM {q(table)} WHERE {where} ORDER BY Id ASC LIMIT %s", tuple(query_params))
            
        return cur.fetchall()


def fetch_by_fk(conn, table: str, fk: str, fk_val: Any) -> List[Dict]:
    with conn.cursor(DictCursor) as cur:
        cur.execute(f"SELECT * FROM {q(table)} WHERE {q(fk)}=%s ORDER BY Id", (fk_val,))
        return cur.fetchall()


def fetch_by_fk_many(conn, table: str, fk: str, fk_vals: Sequence[Any]) -> List[Dict]:
    values = list(dict.fromkeys(v for v in fk_vals if v is not None))
    if not values:
        return []
    rows: List[Dict] = []
    with conn.cursor(DictCursor) as cur:
        for start in range(0, len(values), DB_IN_CLAUSE_CHUNK_SIZE):
            chunk = values[start:start + DB_IN_CLAUSE_CHUNK_SIZE]
            placeholders = ", ".join(["%s"] * len(chunk))
            sql = f"SELECT * FROM {q(table)} WHERE {q(fk)} IN ({placeholders}) ORDER BY Id"
            cur.execute(sql, tuple(chunk))
            rows.extend(cur.fetchall())
    return rows


def fetch_by_ids(conn, table: str, row_ids: Sequence[Any]) -> List[Dict]:
    values = list(dict.fromkeys(v for v in row_ids if v is not None))
    if not values:
        return []
    rows: List[Dict] = []
    with conn.cursor(DictCursor) as cur:
        for start in range(0, len(values), DB_IN_CLAUSE_CHUNK_SIZE):
            chunk = values[start:start + DB_IN_CLAUSE_CHUNK_SIZE]
            placeholders = ", ".join(["%s"] * len(chunk))
            sql = f"SELECT * FROM {q(table)} WHERE Id IN ({placeholders})"
            cur.execute(sql, tuple(chunk))
            rows.extend(cur.fetchall())
    return rows


def group_rows_by_key(rows: Sequence[Dict], key: str) -> Dict[Any, List[Dict]]:
    grouped: Dict[Any, List[Dict]] = {}
    for row in rows:
        row_key = row.get(key)
        if row_key is None:
            continue
        grouped.setdefault(row_key, []).append(row)
    return grouped


def map_rows_by_id(rows: Sequence[Dict]) -> Dict[Any, Dict]:
    return {row.get("Id"): row for row in rows if row.get("Id") is not None}


def sanitize_file_name(name: str) -> str:
    cleaned = (name or "").strip() or "unknown_file.pdf"
    cleaned = re.sub(r'[<>:"/\\|?*]+', "_", cleaned)
    return cleaned


def extract_blob_name(file_url: Any, file_name: Any, container_name: str) -> str:
    file_url_text = (str(file_url).strip() if file_url is not None else "")
    if file_url_text:
        parsed = urlparse(file_url_text)
        path = unquote(parsed.path.lstrip("/"))
        if path:
            marker = f"{container_name}/"
            idx = path.lower().find(marker.lower())
            if idx >= 0:
                return path[idx + len(marker):]
            return path
    return str(file_name).strip() if file_name is not None else ""


def try_download_pdf_for_allocation(
    container_client,
    allocation_row: Dict,
    target_dir: str,
    container_name: str,
) -> str:
    file_name = sanitize_file_name(str(allocation_row.get("File_name") or "unknown_file.pdf"))
    if not file_name.lower().endswith(".pdf"):
        file_name = f"{file_name}.pdf"

    target_path = os.path.join(target_dir, file_name)
    if os.path.exists(target_path):
        return "skipped_existing"

    blob_name = extract_blob_name(
        allocation_row.get("File_url"),
        allocation_row.get("File_name"),
        container_name,
    ).lstrip("/")
    if not blob_name:
        return "failed_missing_blob_name"

    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open(target_path, "wb") as fh:
            fh.write(blob_client.download_blob().readall())
        return "downloaded"
    except Exception:
        return "failed_download"


# ---------------------------------------------------------------------------
# Type formatting – DB values –> RawJson-compatible strings
# ---------------------------------------------------------------------------

def fmt_str(v: Any) -> Optional[str]:
    """Return a cleaned string or None."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def fmt_money(v: Any) -> Optional[str]:
    """Decimal / float / int –> string like '123.45'."""
    if v is None:
        return None
    try:
        d = Decimal(str(v))
        return str(d)
    except (InvalidOperation, ValueError):
        return None


def fmt_date(v: Any) -> Optional[str]:
    """date/datetime –> MM-DD-YYYY string (matches common RawJson format)."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        v = v.date()
    if isinstance(v, dt.date):
        return v.strftime("%m-%d-%Y")
    return fmt_str(v)


def fmt_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


FILE_STATUS_ENUM = {
    0: "Pending", 1: "Failed", 2: "Completed",
    3: "ManuallyCreated", 4: "PartiallyCompleted",
}


def fmt_status(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, int):
        return FILE_STATUS_ENUM.get(v, str(v))
    s = str(v).strip()
    if s.isdigit():
        return FILE_STATUS_ENUM.get(int(s), s)
    return s


# ---------------------------------------------------------------------------
# Build helpers – wrap a DB value in {"value": "..."} to match RawJson shape
# ---------------------------------------------------------------------------

def ocr_node(value: Any) -> Optional[Dict[str, Any]]:
    """Wrap a scalar into the OCR-style {'value': ...} node used in RawJson."""
    if value is None:
        return None
    return {"value": value}


def ocr_node_str(v: Any) -> Optional[Dict[str, Any]]:
    s = fmt_str(v)
    return ocr_node(s) if s is not None else None


def ocr_node_money(v: Any) -> Optional[Dict[str, Any]]:
    s = fmt_money(v)
    return ocr_node(s) if s is not None else None


def ocr_node_date(v: Any) -> Optional[Dict[str, Any]]:
    s = fmt_date(v)
    return ocr_node(s) if s is not None else None


def ocr_node_int(v: Any) -> Optional[Dict[str, Any]]:
    val = fmt_int(v)
    return ocr_node(val) if val is not None else None


def missing_section_value(original: Optional[Dict]) -> Optional[Dict]:
    return original if USE_RAW_FALLBACK_WHEN_DB_MISSING else None


# ---------------------------------------------------------------------------
# Ground-truth builders – mapped specifically to Superbill
# ---------------------------------------------------------------------------

def build_gt_allocation(db_row: Dict, original_alloc: Dict) -> Dict:
    gt = dict(original_alloc)

    # Direct-copy/scalar fields for Allocation level (not wrapped in OCR value nodes)
    gt["File_name"] = db_row.get("File_name")
    gt["File_url"] = db_row.get("File_url")
    gt["Client"] = db_row.get("Client")
    gt["Account"] = db_row.get("Account")
    gt["Total_Charge_Amt_On_File"] = fmt_money(db_row.get("Total_Charge_Amt_On_File"))
    gt["Date_Of_Service"] = fmt_date(db_row.get("Date_Of_Service"))
    gt["Download_Date"] = fmt_date(db_row.get("Download_Date"))
    gt["Completed_Date"] = fmt_date(db_row.get("Completed_Date"))
    gt["Not_Completed_Reason"] = db_row.get("Not_Completed_Reason")
    gt["File_Status"] = fmt_status(db_row.get("File_Status"))

    return gt


def build_gt_patient(db_patient: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
    if db_patient is None:
        return missing_section_value(original)
    gt: Dict[str, Any] = {}
    gt["Patient_FN"]             = ocr_node_str(db_patient.get("Patient_FN"))
    gt["Patient_LN"]             = ocr_node_str(db_patient.get("Patient_LN"))
    gt["Patient_MI"]             = ocr_node_str(db_patient.get("Patient_MI"))
    gt["Patient_Id"]             = ocr_node_str(db_patient.get("Patient_Id"))
    gt["Patient_Account_Number"] = ocr_node_str(db_patient.get("Patient_Account_Number"))
    gt["Patient_Control_Number"] = ocr_node_str(db_patient.get("Patient_Control_Number"))
    gt["Patient_Group"]          = ocr_node_str(db_patient.get("Patient_Group"))
    gt["Patient_Addr1"]          = ocr_node_str(db_patient.get("Patient_Addr1"))
    gt["Patient_Addr2"]          = ocr_node_str(db_patient.get("Patient_Addr2"))
    gt["Patient_City"]           = ocr_node_str(db_patient.get("Patient_City"))
    gt["Patient_State"]          = ocr_node_str(db_patient.get("Patient_State"))
    gt["Patient_Zip"]            = ocr_node_str(db_patient.get("Patient_Zip"))
    gt["Patient_DOB"]            = ocr_node_date(db_patient.get("Patient_DOB"))
    gt["Patient_Gender"]         = ocr_node_str(db_patient.get("Patient_Gender"))
    gt["Patient_Relationship"]   = ocr_node_str(db_patient.get("Patient_Relationship"))
    gt["Patient_Marital_Status"] = ocr_node_str(db_patient.get("Patient_Marital_Status"))
    gt["Patient_Primary_Phone_No"] = ocr_node_str(db_patient.get("Patient_Primary_Phone_No"))
    gt["Patient_Home_Phone_No"]    = ocr_node_str(db_patient.get("Patient_Home_Phone_No"))
    gt["Patient_Primary_Email"]    = ocr_node_str(db_patient.get("Patient_Primary_Email"))
    gt["Insured_Name"]           = ocr_node_str(db_patient.get("Insured_Name"))
    return gt


def build_gt_payer(db_payer: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
    if db_payer is None:
        return missing_section_value(original)
    gt: Dict[str, Any] = {}
    gt["Payer_Name"]  = ocr_node_str(db_payer.get("Payer_Name"))
    gt["Payer_Id"]    = ocr_node_str(db_payer.get("Payer_Id"))
    gt["Payer_Addr1"] = ocr_node_str(db_payer.get("Payer_Addr1"))
    gt["Payer_Addr2"] = ocr_node_str(db_payer.get("Payer_Addr2"))
    gt["Payer_City"]  = ocr_node_str(db_payer.get("Payer_City"))
    gt["Payer_State"] = ocr_node_str(db_payer.get("Payer_State"))
    gt["Payer_Zip"]   = ocr_node_str(db_payer.get("Payer_Zip"))
    gt["Payer_Type"]  = ocr_node_str(db_payer.get("Payer_Type"))
    return gt


def build_gt_provider(db_provider: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
    if db_provider is None:
        return missing_section_value(original)
    gt: Dict[str, Any] = {}
    gt["Provider_Name"]     = ocr_node_str(db_provider.get("Provider_Name"))
    gt["Provider_NPI"]      = ocr_node_str(db_provider.get("Provider_NPI"))
    gt["Provider_Addr1"]    = ocr_node_str(db_provider.get("Provider_Addr1"))
    gt["Provider_Addr2"]    = ocr_node_str(db_provider.get("Provider_Addr2"))
    gt["Provider_City"]     = ocr_node_str(db_provider.get("Provider_City"))
    gt["Provider_State"]    = ocr_node_str(db_provider.get("Provider_State"))
    gt["Provider_Zip"]      = ocr_node_str(db_provider.get("Provider_Zip"))
    gt["Provider_FedId"]    = ocr_node_str(db_provider.get("Provider_FedId"))
    gt["Provider_TaxId"]    = ocr_node_str(db_provider.get("Provider_TaxId"))
    gt["Provider_Taxonomy"] = ocr_node_str(db_provider.get("Provider_Taxonomy"))
    return gt


def build_gt_facility(db_provider: Optional[Dict], original: Optional[Dict]) -> Optional[Dict]:
    if db_provider is None:
        return missing_section_value(original)
    gt: Dict[str, Any] = {}
    gt["Facility_Name"]     = ocr_node_str(db_provider.get("Provider_Name"))
    gt["Facility_NPI"]      = ocr_node_str(db_provider.get("Provider_NPI"))
    gt["Facility_Addr1"]    = ocr_node_str(db_provider.get("Provider_Addr1"))
    gt["Facility_Addr2"]    = ocr_node_str(db_provider.get("Provider_Addr2"))
    gt["Provider_City"]     = ocr_node_str(db_provider.get("Provider_City"))
    gt["Facility_State"]    = ocr_node_str(db_provider.get("Provider_State"))
    gt["Facility_Zip"]      = ocr_node_str(db_provider.get("Provider_Zip"))
    gt["Facility_FedId"]    = ocr_node_str(db_provider.get("Provider_FedId"))
    gt["Facility_TaxId"]    = ocr_node_str(db_provider.get("Provider_TaxId"))
    gt["Facility_Taxonomy"] = ocr_node_str(db_provider.get("Provider_Taxonomy"))
    return gt


def build_gt_diagnosis(db_dx_rows: List[Dict], original: Optional[Dict]) -> Optional[Dict]:
    if not db_dx_rows:
        return missing_section_value(original)
    dx = db_dx_rows[0]
    gt: Dict[str, Any] = {}
    gt["Primary_DX"] = ocr_node_str(dx.get("Primary_DX"))
    for i in range(1, 13):
        key = f"Secondary_DX{i}"
        gt[key] = ocr_node_str(dx.get(key))
    return gt


def build_gt_service_line(db_sl: Dict) -> Dict:
    gt: Dict[str, Any] = {}
    gt["Service_From_Date"]  = ocr_node_date(db_sl.get("Service_From_Date"))
    gt["Service_To_Date"]    = ocr_node_date(db_sl.get("Service_To_Date"))
    gt["Procedure_Code"]     = ocr_node_str(db_sl.get("Procedure_Code"))
    gt["Mod1"]               = ocr_node_str(db_sl.get("Mod1"))
    gt["Mod2"]               = ocr_node_str(db_sl.get("Mod2"))
    gt["Mod3"]               = ocr_node_str(db_sl.get("Mod3"))
    gt["Mod4"]               = ocr_node_str(db_sl.get("Mod4"))
    gt["Service_Billed_Amt"] = ocr_node_money(db_sl.get("Service_Billed_Amt"))
    gt["D_U"]                = ocr_node_int(db_sl.get("D_U"))
    gt["Place_Of_Service"]   = ocr_node_str(db_sl.get("Place_Of_Service"))
    gt["DX_1"]               = ocr_node_str(db_sl.get("DX_1"))
    gt["DX_2"]               = ocr_node_str(db_sl.get("DX_2"))
    gt["DX_3"]               = ocr_node_str(db_sl.get("DX_3"))
    gt["DX_4"]               = ocr_node_str(db_sl.get("DX_4"))
    gt["User_Status"]        = ocr_node_str(db_sl.get("User_Status"))
    return gt


def build_gt_claim(
    db_claim: Dict,
    original_claim: Optional[Dict],
    payer_rows: List[Dict],
    provider_rows: List[Dict],
    patient_by_id: Dict[Any, Dict],
    dx_by_claim_id: Dict[Any, List[Dict]],
    service_lines_by_claim_id: Dict[Any, List[Dict]],
) -> Dict:
    """Build a single Superbill Claim ground-truth dict from DB rows."""
    gt: Dict[str, Any] = {}
    if original_claim:
        gt.update(original_claim)

    # --- Claim-level OCR value fields ---
    gt["Patient_Control_Number"] = ocr_node_str(db_claim.get("Patient_Control_Number"))
    gt["Claim_Total_Charge_Amt"] = ocr_node_money(db_claim.get("Claim_Total_Charge_Amt"))
    gt["Claim_Filing_indicator"] = ocr_node_str(db_claim.get("Claim_Filing_indicator"))
    gt["Claim_Frequency_Code"]   = ocr_node_str(db_claim.get("Claim_Frequency_Code"))
    gt["Claim_Date_of_Service"]  = ocr_node_date(db_claim.get("Claim_Date_of_Service"))
    gt["Claim_Auth_No"]          = ocr_node_str(db_claim.get("Claim_Auth_No"))
    gt["Patient_Paid_Amt"]       = ocr_node_money(db_claim.get("Patient_Paid_Amt"))
    gt["CLIA_Number"]            = ocr_node_str(db_claim.get("CLIA_Number"))
    gt["CLaim_Invoice_Number"]   = ocr_node_str(db_claim.get("CLaim_Invoice_Number"))
    gt["Admission_Date"]         = ocr_node_date(db_claim.get("Admission_Date"))
    gt["Patient_Last_Seen_Date"] = ocr_node_date(db_claim.get("Patient_Last_Seen_Date"))

    # --- Sub-entities: Patient ---
    gt["Patient"] = build_gt_patient(
        patient_by_id.get(db_claim.get("PatientId")),
        original_claim.get("Patient") if original_claim else None,
    )

    # --- Sub-entities: Payer (List) ---
    original_payers = (original_claim.get("Payer") or []) if original_claim else []
    gt_payers = []
    for idx, db_payer in enumerate(payer_rows):
        orig_p = original_payers[idx] if idx < len(original_payers) else None
        gt_payers.append(build_gt_payer(db_payer, orig_p))
    gt["Payer"] = gt_payers

    # --- Sub-entities: Providers (By Role Key Mapping) ---
    # Map roles based on provider roles definition
    role_to_provider = {prov.get("Role"): prov for prov in provider_rows if prov.get("Role")}
    
    gt["BillingProvider"] = build_gt_provider(
        role_to_provider.get("BillingProvider"),
        original_claim.get("BillingProvider") if original_claim else None
    )
    gt["ServicingProvider"] = build_gt_provider(
        role_to_provider.get("ServicingProvider"),
        original_claim.get("ServicingProvider") if original_claim else None
    )
    gt["ReferringProvider"] = build_gt_provider(
        role_to_provider.get("ReferringProvider"),
        original_claim.get("ReferringProvider") if original_claim else None
    )
    gt["OrderingProvider"] = build_gt_provider(
        role_to_provider.get("OrderingProvider"),
        original_claim.get("OrderingProvider") if original_claim else None
    )
    gt["SupervisingProvider"] = build_gt_provider(
        role_to_provider.get("SupervisingProvider"),
        original_claim.get("SupervisingProvider") if original_claim else None
    )
    gt["ServicingFacility"] = build_gt_facility(
        role_to_provider.get("ServiceFacilityProvider"),
        original_claim.get("ServicingFacility") if original_claim else None
    )

    # --- Diagnosis ---
    claim_id = db_claim.get("Id")
    db_dx = dx_by_claim_id.get(claim_id, [])
    gt["ClaimDiagnosisCodes"] = build_gt_diagnosis(
        db_dx,
        original_claim.get("ClaimDiagnosisCodes") if original_claim else None,
    )

    # --- Service Lines ---
    db_sls = service_lines_by_claim_id.get(claim_id, [])
    gt_sls = []
    for db_sl in db_sls:
        gt_sls.append(build_gt_service_line(db_sl))
    gt["ServiceLines"] = gt_sls

    return gt


# ---------------------------------------------------------------------------
# JSON serializer for Decimal / date / datetime
# ---------------------------------------------------------------------------

class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.strftime("%m-%d-%Y")
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)


def dated_json_filename(filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    if not ext:
        ext = ".json"
        stem = filename
    run_date = dt.datetime.now().strftime("%Y%m%d")
    return f"{stem}_{run_date}{ext}"


# ---------------------------------------------------------------------------
# Service Bus Queue Dispatch Helper
# ---------------------------------------------------------------------------

def _send_superbill_records_to_queue(
    records: List[Dict[str, Any]],
    ids_per_message: int,
    max_messages_per_run: Optional[int],
    folder_name: str = "main",
    progress_log_batch_size: int = 10,
) -> int:
    """Send Superbill fine-tuning records to Service Bus queue in chunked messages."""
    queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "").strip()
    conn_string = os.getenv("SERVICE_BUS_CONNECTION_STRING", "").strip()
    
    if not queue_name or not conn_string:
        logging.warning("[Queue] Skipping: SERVICE_BUS_QUEUE_NAME or SERVICE_BUS_CONNECTION_STRING not set")
        return 0
    
    if not records:
        logging.info("[Queue] No records to send")
        return 0
    
    try:
        from azure.servicebus import ServiceBusClient, ServiceBusMessage
        
        client = ServiceBusClient.from_connection_string(conn_string)
        messages_sent = 0
        records_dispatched = 0

        chunks = [
            records[i:i + ids_per_message]
            for i in range(0, len(records), ids_per_message)
        ]
        # Treat -1 as unlimited
        if max_messages_per_run is not None and max_messages_per_run != -1 and max_messages_per_run > 0:
            chunks = chunks[:max_messages_per_run]
        
        with client.get_queue_sender(queue_name, socket_timeout=10.0) as sender:
            for i, chunk in enumerate(chunks):
                allocation_ids = [rec.get("allocation_id") for rec in chunk]
                chunk_payload = [
                    {
                        "file_name": rec.get("file_name"),
                        "allocation_id": rec.get("allocation_id"),
                        "ground_truth": rec.get("ground_truth"),
                    }
                    for rec in chunk
                ]

                try:
                    body = {
                        "file_name": chunk_payload[0].get("file_name") if len(chunk_payload) == 1 else None,
                        "allocation_id": chunk_payload[0].get("allocation_id") if len(chunk_payload) == 1 else None,
                        "ground_truth": chunk_payload[0].get("ground_truth") if len(chunk_payload) == 1 else None,
                        "allocation_ids": allocation_ids,
                        "records": chunk_payload,
                        "source": "healthcare_superbill",
                        "container": os.getenv("SUPERBILL_CONTAINER", "superbill-dataset"),
                        "folder_name": folder_name,
                        "environment": os.getenv("LANGFUSE_ENVIRONMENT", "exp"),
                        "process_type": "FineTuning",
                        "queued_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    }
                    msg = ServiceBusMessage(json.dumps(body, cls=_Encoder))
                    sender.send_messages([msg])
                    messages_sent += 1
                    records_dispatched += len(chunk)
                    
                    if (i + 1) % progress_log_batch_size == 0:
                        logging.info(f"[Queue] Sent {messages_sent}/{len(chunks)} messages; records dispatched={records_dispatched}")
                except Exception as e:
                    logging.warning(f"[Queue] Failed to send chunk with allocation_ids={allocation_ids}: {e}")
            
            logging.info(f"[Queue] SUCCESS: {messages_sent}/{len(chunks)} messages sent; records dispatched={records_dispatched}")
            return messages_sent
    except Exception as e:
        logging.error(f"[Queue] Error sending to queue: {e}")
        return 0


def superbill_fine_tuning_data_push(
    ids_per_message: Optional[int] = None,
    max_messages_per_run: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    folder_name: Optional[str] = None,
    bypass_checkpoint: bool = False
) -> None:
    """
    Generate Superbill fine-tuning data.
    Callable by Azure Functions triggers as well as manual standalone executions.
    
    Args:
        ids_per_message: IDs per message (optional, uses env if not provided)
        max_messages_per_run: Max messages per run (optional, uses env if not provided)
        start_date: Start date in format 'YYYY-MM-DD HH:MM:SS' (optional)
        end_date: End date in format 'YYYY-MM-DD HH:MM:SS' (optional)
        folder_name: Logical folder/group name for checkpoint isolation and output (default: 'main')
        bypass_checkpoint: If True, ignore checkpoint and use start_date/end_date; if False, compare checkpoint with start_date
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv_file(os.path.join(script_dir, ".env"))
    try:
        runtime_cfg = apply_env_overrides()
    except Exception as ex:
        logging.error(f"Config error: {ex}")
        raise ex

    outdir_name = runtime_cfg["output_dir"]
    pdf_subdir = runtime_cfg["pdf_output_subdir"]

    env_ids_per_message = (os.getenv("IDS_PER_MESSAGE") or "").strip()
    eff_ids_per_message = ids_per_message if ids_per_message is not None else int(env_ids_per_message or "1")
    if eff_ids_per_message <= 0:
        raise ValueError("ids_per_message must be greater than 0")

    env_max_messages = (os.getenv("MAX_MESSAGES_PER_RUN") or "").strip()
    eff_max_messages_per_run = (
        (None if max_messages_per_run == -1 else max_messages_per_run)  # Treat -1 as unlimited
        if max_messages_per_run is not None
        else int(env_max_messages) if env_max_messages else None
    )
    if eff_max_messages_per_run is not None and eff_max_messages_per_run <= 0:
        raise ValueError("max_messages_per_run must be greater than 0 when provided")

    logging.info(
        "[Config] Effective queue batching: ids_per_message=%s, max_messages_per_run=%s",
        eff_ids_per_message,
        eff_max_messages_per_run,
    )
    
    # folder_name isolates checkpoint and output data — defaults to 'main'
    folder_name = (folder_name or "main").strip()
    logging.info(f"[Config] Using folder_name: {folder_name}")
    
    # Handle dates and checkpoint logic
    logging.info(f"[Config] start_date={start_date}, end_date={end_date}, bypass_checkpoint={bypass_checkpoint}")
    
    effective_start_date = start_date
    effective_end_date = end_date or dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get database config first for oldest date lookup
    try:
        cfg = resolve_db_config()
    except Exception as ex:
        logging.error(f"Config error: {ex}")
        raise ex
    
    # If start_date not provided, fetch oldest date from database
    if not effective_start_date:
        logging.info("[Config] start_date not provided, fetching oldest date from database...")
        effective_start_date = _get_oldest_date_from_db(cfg)
        if effective_start_date:
            logging.info(f"[Config] Using oldest date from database: {effective_start_date}")
        else:
            logging.warning("[Config] Could not determine oldest date, using current date")
            effective_start_date = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -----------------------------------------------------
    # Langfuse Checkpoint Init
    # -----------------------------------------------------
    langfuse_environment = os.getenv("LANGFUSE_ENVIRONMENT", "exp").strip()
    checkpoint_dataset_name = f"superbill_finetuning_checkpoint_{folder_name}"
    logging.info(f"[Config] Using checkpoint table: {checkpoint_dataset_name}")
    
    last_checkpoint_id = None if bypass_checkpoint else _load_checkpoint_from_langfuse(checkpoint_dataset_name)
    
    if bypass_checkpoint:
        logging.info("[Fine Tuning Task] Checkpoint bypass is enabled, using provided start_date/end_date")

    outdir = outdir_name if os.path.isabs(outdir_name) else os.path.join(script_dir, outdir_name)
    pdf_output_dir = os.path.join(outdir, pdf_subdir)
    download_enabled = False
    container_client = None

    if DOWNLOAD_PDFS:
        try:
            from azure.storage.blob import BlobServiceClient
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING_HEALTHCARE_AI", "").strip()
            container_name = os.getenv("HEALTHCARE_AI_CONTAINER_NAME_SUPERBILL", "").strip()
            os.makedirs(pdf_output_dir, exist_ok=True)
            blob_service = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service.get_container_client(container_name)
            download_enabled = True
        except Exception as ex:
            logging.error(f"Config error: could not initialize Azure Blob client: {ex}")
            raise ex

    try:
        conn = pymysql.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            database=cfg.database,
            cursorclass=DictCursor, autocommit=True,
        )
    except Exception as ex:
        logging.error(f"DB connection error: {ex}")
        raise ex

    with conn:
        try:
            tables = {
                "allocation": resolve_table_name(conn, ["SuperBillAllocations"]),
                "claim":      resolve_table_name(conn, ["SuperBillClaims"]),
                "payer":      resolve_table_name(conn, ["SuperBillPayers"]),
                "provider":   resolve_table_name(conn, ["SuperBillClaimProviders"]),
                "patient":    resolve_table_name(conn, ["PatientRecords"]),
                "dx":         resolve_table_name(conn, ["SuperBillDiagnosisCodes"]),
                "sl":         resolve_table_name(conn, ["SuperBillServiceLines"]),
            }
        except Exception as ex:
            logging.error(f"Table resolution error: {ex}")
            raise ex

        # When unlimited (-1 → None), fetch ALL rows above checkpoint; otherwise cap at max_messages × ids_per_message
        fetch_all = eff_max_messages_per_run is None
        max_rows = (eff_ids_per_message * eff_max_messages_per_run) if eff_max_messages_per_run is not None else 0

        # Fetch using checkpoint-aware DB query
        allocations = fetch_allocations(conn, tables["allocation"], last_checkpoint_id, max_rows, fetch_all)
        
        if not allocations:
            logging.info("No allocations found matching the current checkpoint.")
            return

        # --- Batch pre-fetch: load ALL child records for the entire allocation batch in one round ---
        # Before: N_allocations × 6 queries per allocation (N+1 problem)
        # After:  7 queries total regardless of batch size
        batch_alloc_ids = [a.get("Id") for a in allocations if a.get("Id") is not None]

        all_claims = fetch_by_fk_many(conn, tables["claim"], "AllocationRecordId", batch_alloc_ids)
        claims_by_alloc_id = group_rows_by_key(all_claims, "AllocationRecordId")
        all_claim_ids = [c.get("Id") for c in all_claims if c.get("Id") is not None]
        claim_to_alloc_id = {c.get("Id"): c.get("AllocationRecordId") for c in all_claims if c.get("Id")}

        all_patient_ids = list({c.get("PatientId") for c in all_claims if c.get("PatientId") is not None})
        batch_patient_by_id = map_rows_by_id(fetch_by_ids(conn, tables["patient"], all_patient_ids))

        batch_payers_by_claim = group_rows_by_key(
            fetch_by_fk_many(conn, tables["payer"], "ClaimRecordId", all_claim_ids), "ClaimRecordId"
        )
        batch_providers_by_claim = group_rows_by_key(
            fetch_by_fk_many(conn, tables["provider"], "ClaimRecordId", all_claim_ids), "ClaimRecordId"
        )
        batch_dx_by_claim_id = group_rows_by_key(
            fetch_by_fk_many(conn, tables["dx"], "ClaimRecordId", all_claim_ids), "ClaimRecordId"
        )
        all_service_lines = fetch_by_fk_many(conn, tables["sl"], "ClaimRecordId", all_claim_ids)
        batch_sl_by_claim_id = group_rows_by_key(all_service_lines, "ClaimRecordId")

        # Group service lines by allocation ID (for per-allocation count in audit)
        batch_sl_by_alloc_id: Dict[Any, List[Dict]] = {}
        for _sl in all_service_lines:
            _cid = _sl.get("ClaimRecordId")
            _aid = claim_to_alloc_id.get(_cid)
            if _aid is not None:
                batch_sl_by_alloc_id.setdefault(_aid, []).append(_sl)

        logging.info(
            "[Superbill] Pre-fetch complete: %d allocations, %d claims, %d service lines",
            len(batch_alloc_ids), len(all_claims), len(all_service_lines),
        )
        # ---------------------------------------------------------------------------------

        written = 0
        skipped = 0
        audit_mismatches = 0
        downloaded_pdfs = 0
        skipped_pdfs = 0
        failed_pdfs = 0
        outputs: List[Dict[str, Any]] = []
        total_queued = 0
        batch_count = 0

        # Process in ASCENDING order of allocation ID
        for alloc_row in sorted(allocations, key=lambda r: r.get("Id") or 0, reverse=False):
            alloc_id = alloc_row.get("Id")

            if download_enabled and container_client is not None:
                download_status = try_download_pdf_for_allocation(
                    container_client,
                    alloc_row,
                    pdf_output_dir,
                    container_name,
                )
                if download_status == "downloaded":
                    downloaded_pdfs += 1
                elif download_status == "skipped_existing":
                    skipped_pdfs += 1
                else:
                    failed_pdfs += 1

            file_name = alloc_row.get("File_name") or f"allocation_{alloc_id}"

            raw_json_str = alloc_row.get("RawJson") or alloc_row.get("rawJson")
            if not raw_json_str:
                logging.info(f"  SKIP AllocationId={alloc_id}: empty RawJson")
                skipped += 1
                continue

            try:
                root = json.loads(raw_json_str)
            except json.JSONDecodeError as ex:
                logging.info(f"  SKIP AllocationId={alloc_id}: RawJson parse error – {ex}")
                skipped += 1
                continue

            original_alloc = root.get("Allocation")
            if not isinstance(original_alloc, dict):
                logging.info(f"  SKIP AllocationId={alloc_id}: RawJson missing Allocation object")
                skipped += 1
                continue

            # --- Build ground-truth Allocation ---
            gt_alloc = build_gt_allocation(alloc_row, original_alloc)

            # --- Build ground-truth Claims ---
            original_claims = []
            for ci in original_alloc.get("Claim_Info") or []:
                if isinstance(ci, dict) and isinstance(ci.get("Claim"), dict):
                    original_claims.append(ci["Claim"])

            # Use pre-fetched data — no per-allocation DB queries
            db_claims = claims_by_alloc_id.get(alloc_id, [])
            claim_ids = [c.get("Id") for c in db_claims if c.get("Id") is not None]

            patient_by_id = batch_patient_by_id
            payers_by_claim = batch_payers_by_claim
            providers_by_claim = batch_providers_by_claim
            dx_by_claim_id = batch_dx_by_claim_id
            service_lines = batch_sl_by_alloc_id.get(alloc_id, [])
            service_lines_by_claim_id = batch_sl_by_claim_id

            gt_claims_info = []
            for idx, db_claim in enumerate(db_claims):
                orig = original_claims[idx] if idx < len(original_claims) else None
                claim_id = db_claim.get("Id")
                
                gt_claim = build_gt_claim(
                    db_claim,
                    orig,
                    payers_by_claim.get(claim_id, []),
                    providers_by_claim.get(claim_id, []),
                    patient_by_id,
                    dx_by_claim_id,
                    service_lines_by_claim_id,
                )
                gt_claims_info.append({"Claim": gt_claim})

            gt_alloc["Claim_Info"] = gt_claims_info

            db_claim_count = len(db_claims)
            out_claim_count = len(gt_claims_info)
            db_service_line_count = len(service_lines)
            out_service_line_count = sum(
                len((claim_info.get("Claim") or {}).get("ServiceLines") or [])
                for claim_info in gt_claims_info
            )
            has_mismatch = (
                db_claim_count != out_claim_count
                or db_service_line_count != out_service_line_count
            )
            if STRICT_AUDIT_MODE:
                status = "MISMATCH" if has_mismatch else "OK"
                logging.info(
                    f"  AUDIT AllocationId={alloc_id}: "
                    f"claims db={db_claim_count}, out={out_claim_count}; "
                    f"service_lines db={db_service_line_count}, out={out_service_line_count} [{status}]"
                )
            if has_mismatch:
                audit_mismatches += 1

            # --- Assemble output ---
            output = {
                "file_name": file_name,
                "allocation_id": alloc_id,
                "raw_json": root,
                "ground_truth": {"Allocation": gt_alloc},
            }

            outputs.append(output)
            written += 1
            logging.info(f"  OK AllocationId={alloc_id}")

            # Send to queue immediately when a full batch is ready
            if len(outputs) >= eff_ids_per_message:
                batch_count += 1
                logging.info(f"[Superbill] Sending batch {batch_count} ({len(outputs)} records)...")
                try:
                    sent = _send_superbill_records_to_queue(
                        outputs,
                        ids_per_message=eff_ids_per_message,
                        max_messages_per_run=None,
                        folder_name=folder_name,
                    )
                    total_queued += sent
                    last_id = outputs[-1].get("allocation_id")
                    if last_id:
                        _save_checkpoint_to_langfuse(checkpoint_dataset_name, last_id)
                        logging.info(f"[Superbill] Batch {batch_count} sent. Checkpoint saved: {last_id}")
                except Exception as e:
                    logging.error(f"[Superbill] Error sending batch {batch_count}: {e}", exc_info=True)
                finally:
                    outputs.clear()

    # --- Send any remaining records (partial final batch) ---
    if outputs:
        batch_count += 1
        logging.info(f"[Superbill] Sending final batch {batch_count} ({len(outputs)} records)...")
        try:
            sent = _send_superbill_records_to_queue(
                outputs,
                ids_per_message=eff_ids_per_message,
                max_messages_per_run=None,
                folder_name=folder_name,
            )
            total_queued += sent
            last_id = outputs[-1].get("allocation_id")
            if last_id:
                _save_checkpoint_to_langfuse(checkpoint_dataset_name, last_id)
                logging.info(f"[Superbill] Final batch {batch_count} sent. Checkpoint saved: {last_id}")
        except Exception as e:
            logging.error(f"[Superbill] Error sending final batch {batch_count}: {e}", exc_info=True)

    queue_count = total_queued

    logging.info(f"Done. Written: {written}, Skipped: {skipped}")
    if download_enabled:
        logging.info(f"PDFs: downloaded={downloaded_pdfs}, skipped_existing={skipped_pdfs}, failed={failed_pdfs}")
        logging.info(f"PDF folder: {pdf_output_dir}")
    if STRICT_AUDIT_MODE:
        logging.info(f"Audit mismatches: {audit_mismatches}")
    if STRICT_AUDIT_MODE and STRICT_AUDIT_FAIL_ON_MISMATCH and audit_mismatches > 0:
        raise ValueError("Strict audit failed due to mismatch in claim/service-line counts.")
    logging.info(f"Queued: {queue_count} records to Service Bus")


if __name__ == "__main__":
    logging.info("Running Superbill Fine Tuning manually...")
    try:
        superbill_fine_tuning_data_push()
        sys.exit(0)
    except Exception as main_err:
        logging.critical(f"FATAL: {main_err}", exc_info=True)
        sys.exit(1)