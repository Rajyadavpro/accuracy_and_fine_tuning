#!/usr/bin/env python3
"""Standalone Superbill healthcare accuracy runner."""

from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pymysql
from pymysql.cursors import DictCursor

from accuracy.healthcare_accuracy import AuditSummary, CompareStats, DbConfig, LANGFUSE_ENVIRONMENT, MAX_ROWS
from accuracy.healthcare_accuracy import MAX_WORKERS, PAGE_SIZE, RESUME_LOOKBACK_ALLOCATIONS, SUPERBILL_DATASET_NAME
from accuracy.healthcare_accuracy import UPLOAD_EACH_BATCH, UPLOAD_EVERY_ROWS, _allocation_sort_key, _extract_date_string
from accuracy.healthcare_accuracy import _fetch_allocations_after_id, _fetch_max_allocation_id, _is_allocation_already_uploaded
from accuracy.healthcare_accuracy import _legacy_dataset_item_id, _load_langfuse_dataset_state, _log, _resolve_row_limit
from accuracy.healthcare_accuracy import _stable_dataset_item_id, _to_iso_datetime_string, _upload_rows_to_langfuse
from accuracy.healthcare_accuracy import alnum_only, digits_only, fetch_row_by_id, fetch_rows_by_fk, is_diagnosis_path
from accuracy.healthcare_accuracy import is_phone_path, is_zip_path, load_dotenv_file, map_file_status, normalize_compare_str
from accuracy.healthcare_accuracy import normalize_enum, ocr_value, ocr_value_any, parse_date, parse_int, parse_money
from accuracy.healthcare_accuracy import parse_str, print_summary, resolve_db_config, resolve_table_name, should_ignore_mismatch
from accuracy.healthcare_accuracy import to_date_db, to_decimal_db, value_str


_SB_ACTIVE_STATS = threading.local()
_SUPERBILL_WORKER_LOCAL = threading.local()
_SUPERBILL_WORKER_CONNS: list[pymysql.connections.Connection] = []
_SUPERBILL_WORKER_CONNS_LOCK = threading.Lock()


def sb_add_mismatch(message: str, mismatches: list[str]) -> None:
    if should_ignore_mismatch(message):
        return
    mismatches.append(message)
    active_stats = getattr(_SB_ACTIVE_STATS, "value", None)
    if active_stats is not None:
        active_stats.mismatches += 1


def sb_eq(path: str, actual: object, expected: object, mismatches: list[str]) -> None:
    active_stats = getattr(_SB_ACTIVE_STATS, "value", None)
    if actual == expected:
        if active_stats is not None:
            active_stats.matches += 1
        return
    if isinstance(actual, str) and isinstance(expected, str):
        actual_norm = normalize_compare_str(actual)
        expected_norm = normalize_compare_str(expected)
        if actual_norm == expected_norm:
            if active_stats is not None:
                active_stats.matches += 1
            return
        if is_diagnosis_path(path) and alnum_only(actual_norm) == alnum_only(expected_norm):
            if active_stats is not None:
                active_stats.matches += 1
            return
        if is_phone_path(path) and digits_only(actual_norm) == digits_only(expected_norm):
            if active_stats is not None:
                active_stats.matches += 1
            return
        if is_zip_path(path) and digits_only(actual_norm) == digits_only(expected_norm):
            if active_stats is not None:
                active_stats.matches += 1
            return
    sb_add_mismatch(f"{path} mismatch. expected={value_str(expected)}, actual={value_str(actual)}", mismatches)


def _get_superbill_worker_resources(cfg: DbConfig) -> tuple[pymysql.connections.Connection, dict[str, str]]:
    conn = getattr(_SUPERBILL_WORKER_LOCAL, "conn", None)
    tables = getattr(_SUPERBILL_WORKER_LOCAL, "tables", None)
    if conn is not None and tables is not None:
        return conn, tables

    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        cursorclass=DictCursor,
        autocommit=True,
    )
    tables = {
        "allocation": resolve_table_name(conn, ["AllocationRecord", "SuperBillAllocation", "SuperBillAllocations"]),
        "claim": resolve_table_name(conn, ["ClaimRecord", "SuperBillClaim", "SuperBillClaims"]),
        "payer": resolve_table_name(conn, ["PayerRecord", "SuperBillPayer", "SuperBillPayers"]),
        "provider": resolve_table_name(conn, ["ClaimProviderRecord", "SuperBillClaimProvider", "SuperBillClaimProviders"]),
        "patient": resolve_table_name(conn, ["PatientRecord", "PatientRecords"]),
        "dx": resolve_table_name(conn, ["DiagnosisCodesRecord", "SuperBillDiagnosisCode", "SuperBillDiagnosisCodes"]),
        "sl": resolve_table_name(conn, ["ServiceLineRecord", "SuperBillServiceLine", "SuperBillServiceLines"]),
    }
    _SUPERBILL_WORKER_LOCAL.conn = conn
    _SUPERBILL_WORKER_LOCAL.tables = tables
    with _SUPERBILL_WORKER_CONNS_LOCK:
        _SUPERBILL_WORKER_CONNS.append(conn)
    return conn, tables


def _close_superbill_worker_connections() -> None:
    with _SUPERBILL_WORKER_CONNS_LOCK:
        connections = list(_SUPERBILL_WORKER_CONNS)
        _SUPERBILL_WORKER_CONNS.clear()
    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass


def _build_superbill_output_row(row: dict[str, object], stats: CompareStats) -> dict[str, object]:
    allocation_total = stats.matches + stats.mismatches
    accuracy_pct = (stats.matches / allocation_total * 100.0) if allocation_total else 0.0
    return {
        "date": _extract_date_string(row.get("Download_Date") or row.get("download_date")),
        "accuracy": round(accuracy_pct, 4),
        "date_time": _to_iso_datetime_string(row.get("UpdatedAt") or row.get("UpdatedOn") or row.get("CreatedOn")),
        "file_name": row.get("File_name") or row.get("file_name"),
        "client_name": row.get("Client") or row.get("client_name"),
        "allocation_id": row.get("Id"),
        "total_matched": stats.matches,
        "eob_or_superbill": "Superbill",
        "total_mismatches": stats.mismatches,
        "total_matches": stats.matches,
        "accuracy_pct": accuracy_pct,
        "download_date": _to_iso_datetime_string(row.get("Download_Date") or row.get("download_date")),
        "source_type": "Superbill",
    }


def _process_superbill_row_parallel(row: dict[str, object], cfg: DbConfig) -> tuple[int, CompareStats, dict[str, object], float]:
    row_started = time.perf_counter()
    conn, tables = _get_superbill_worker_resources(cfg)
    _, stats = audit_superbill_allocation(row, conn, tables)
    output_row = _build_superbill_output_row(row, stats)
    row_id = int(row.get("Id") or 0)
    elapsed = time.perf_counter() - row_started
    return row_id, stats, output_row, elapsed


def audit_superbill_allocation(
    allocation_row: dict[str, object],
    conn: pymysql.connections.Connection,
    tables: dict[str, str],
) -> tuple[list[str], CompareStats]:
    mismatches: list[str] = []
    stats = CompareStats()
    _SB_ACTIVE_STATS.value = stats

    raw_json = allocation_row.get("RawJson") or allocation_row.get("rawJson")
    if not raw_json:
        sb_add_mismatch("RawJson missing on allocation row.", mismatches)
        _SB_ACTIVE_STATS.value = None
        return mismatches, stats

    try:
        import json

        root = json.loads(raw_json)
    except Exception as ex:
        sb_add_mismatch(f"RawJson parse error: {ex}", mismatches)
        _SB_ACTIVE_STATS.value = None
        return mismatches, stats

    allocation = root.get("Allocation")
    if not isinstance(allocation, dict):
        sb_add_mismatch("RawJson does not contain Allocation object.", mismatches)
        _SB_ACTIVE_STATS.value = None
        return mismatches, stats

    sb_eq("Allocation.File_name", allocation_row.get("File_name"), allocation.get("File_name"), mismatches)
    sb_eq("Allocation.File_url", allocation_row.get("File_url"), allocation.get("File_url"), mismatches)
    sb_eq("Allocation.Client", allocation_row.get("Client"), allocation.get("Client"), mismatches)
    sb_eq("Allocation.Account", allocation_row.get("Account"), allocation.get("Account"), mismatches)
    sb_eq("Allocation.Total_No_Of_Clm_On_File", allocation_row.get("Total_No_Of_Clm_On_File"), len(allocation.get("Claim_Info") or []), mismatches)
    sb_eq("Allocation.Total_Charge_Amt_On_File", to_decimal_db(allocation_row.get("Total_Charge_Amt_On_File")), parse_money(allocation.get("Total_Charge_Amt_On_File")), mismatches)
    sb_eq("Allocation.Date_Of_Service", to_date_db(allocation_row.get("Date_Of_Service")), parse_date(allocation.get("Date_Of_Service")), mismatches)
    sb_eq("Allocation.Download_Date", to_date_db(allocation_row.get("Download_Date")), parse_date(allocation.get("Download_Date")), mismatches)
    sb_eq("Allocation.Completed_Date", to_date_db(allocation_row.get("Completed_Date")), parse_date(allocation.get("Completed_Date")), mismatches)
    sb_eq("Allocation.Not_Completed_Reason", allocation_row.get("Not_Completed_Reason"), allocation.get("Not_Completed_Reason"), mismatches)
    sb_eq("Allocation.File_Status", normalize_enum(allocation_row.get("File_Status")), map_file_status(allocation.get("File_Status")), mismatches)

    claim_wrappers = allocation.get("Claim_Info") or []
    expected_claims: list[dict[str, object]] = []
    for wrapper in claim_wrappers:
        if isinstance(wrapper, dict) and isinstance(wrapper.get("Claim"), dict):
            expected_claims.append(wrapper["Claim"])

    db_claims = fetch_rows_by_fk(conn, tables["claim"], "AllocationRecordId", allocation_row["Id"], order_by="Id")
    if len(db_claims) != len(expected_claims):
        sb_add_mismatch(f"Claims count mismatch. expected={len(expected_claims)}, actual={len(db_claims)}", mismatches)

    for idx, (db_claim, claim_obj) in enumerate(zip(db_claims, expected_claims)):
        prefix = f"Claim[{idx}]"
        sb_eq(f"{prefix}.Patient_Control_Number", db_claim.get("Patient_Control_Number"), parse_str(ocr_value(claim_obj.get("Patient_Control_Number"))), mismatches)
        sb_eq(f"{prefix}.Claim_Total_Charge_Amt", to_decimal_db(db_claim.get("Claim_Total_Charge_Amt")), parse_money(ocr_value(claim_obj.get("Claim_Total_Charge_Amt"))), mismatches)
        sb_eq(f"{prefix}.Claim_Filing_indicator", db_claim.get("Claim_Filing_indicator"), parse_str(ocr_value(claim_obj.get("Claim_Filing_indicator"))), mismatches)
        sb_eq(f"{prefix}.Claim_Frequency_Code", db_claim.get("Claim_Frequency_Code"), parse_str(ocr_value(claim_obj.get("Claim_Frequency_Code"))), mismatches)
        sb_eq(f"{prefix}.Claim_Date_of_Service", to_date_db(db_claim.get("Claim_Date_of_Service")), parse_date(ocr_value(claim_obj.get("Claim_Date_of_Service"))), mismatches)
        sb_eq(f"{prefix}.Claim_Auth_No", db_claim.get("Claim_Auth_No"), parse_str(ocr_value(claim_obj.get("Claim_Auth_No"))), mismatches)
        sb_eq(f"{prefix}.Patient_Paid_Amt", to_decimal_db(db_claim.get("Patient_Paid_Amt")), parse_money(ocr_value(claim_obj.get("Patient_Paid_Amt"))), mismatches)
        sb_eq(f"{prefix}.CLIA_Number", db_claim.get("CLIA_Number"), parse_str(ocr_value(claim_obj.get("CLIA_Number"))), mismatches)
        sb_eq(f"{prefix}.CLaim_Invoice_Number", db_claim.get("CLaim_Invoice_Number"), parse_str(ocr_value(claim_obj.get("CLaim_Invoice_Number"))), mismatches)
        sb_eq(f"{prefix}.Admission_Date", to_date_db(db_claim.get("Admission_Date")), parse_date(ocr_value(claim_obj.get("Admission_Date"))), mismatches)
        sb_eq(f"{prefix}.Patient_Last_Seen_Date", to_date_db(db_claim.get("Patient_Last_Seen_Date")), parse_date(ocr_value(claim_obj.get("Patient_Last_Seen_Date"))), mismatches)

        expected_patient = claim_obj.get("Patient")
        db_patient = fetch_row_by_id(conn, tables["patient"], db_claim.get("PatientId")) if db_claim.get("PatientId") else None
        if isinstance(expected_patient, dict):
            if db_patient is None:
                sb_add_mismatch(f"{prefix}.Patient missing in DB (expected present).", mismatches)
            else:
                p = f"{prefix}.Patient"
                sb_eq(f"{p}.Patient_FN", db_patient.get("Patient_FN"), parse_str(ocr_value(expected_patient.get("Patient_FN"))), mismatches)
                sb_eq(f"{p}.Patient_LN", db_patient.get("Patient_LN"), parse_str(ocr_value(expected_patient.get("Patient_LN"))), mismatches)
                sb_eq(f"{p}.Patient_MI", db_patient.get("Patient_MI"), parse_str(ocr_value(expected_patient.get("Patient_MI"))), mismatches)
                sb_eq(f"{p}.Patient_Id", db_patient.get("Patient_Id"), parse_str(ocr_value(expected_patient.get("Patient_Id"))), mismatches)
                sb_eq(f"{p}.Patient_Account_Number", db_patient.get("Patient_Account_Number"), parse_str(ocr_value(expected_patient.get("Patient_Account_Number"))), mismatches)
                sb_eq(f"{p}.Patient_Control_Number", db_patient.get("Patient_Control_Number"), parse_str(ocr_value(expected_patient.get("Patient_Control_Number"))), mismatches)
                sb_eq(f"{p}.Patient_Group", db_patient.get("Patient_Group"), parse_str(ocr_value(expected_patient.get("Patient_Group"))), mismatches)
                sb_eq(f"{p}.Patient_Addr1", db_patient.get("Patient_Addr1"), parse_str(ocr_value(expected_patient.get("Patient_Addr1"))), mismatches)
                sb_eq(f"{p}.Patient_Addr2", db_patient.get("Patient_Addr2"), parse_str(ocr_value(expected_patient.get("Patient_Addr2"))), mismatches)
                sb_eq(f"{p}.Patient_City", db_patient.get("Patient_City"), parse_str(ocr_value(expected_patient.get("Patient_City"))), mismatches)
                sb_eq(f"{p}.Patient_State", db_patient.get("Patient_State"), parse_str(ocr_value(expected_patient.get("Patient_State"))), mismatches)
                sb_eq(f"{p}.Patient_Zip", db_patient.get("Patient_Zip"), parse_str(ocr_value(expected_patient.get("Patient_Zip"))), mismatches)
                sb_eq(f"{p}.Patient_DOB", to_date_db(db_patient.get("Patient_DOB")), parse_date(ocr_value(expected_patient.get("Patient_DOB"))), mismatches)
                sb_eq(f"{p}.Patient_Gender", db_patient.get("Patient_Gender"), parse_str(ocr_value(expected_patient.get("Patient_Gender"))), mismatches)
                sb_eq(f"{p}.Patient_Relationship", db_patient.get("Patient_Relationship"), parse_str(ocr_value(expected_patient.get("Patient_Relationship"))), mismatches)
                sb_eq(f"{p}.Patient_Marital_Status", db_patient.get("Patient_Marital_Status"), parse_str(ocr_value(expected_patient.get("Patient_Marital_Status"))), mismatches)
                sb_eq(f"{p}.Patient_Primary_Phone_No", db_patient.get("Patient_Primary_Phone_No"), parse_str(ocr_value(expected_patient.get("Patient_Primary_Phone_No"))), mismatches)
                sb_eq(f"{p}.Patient_Home_Phone_No", db_patient.get("Patient_Home_Phone_No"), parse_str(ocr_value(expected_patient.get("Patient_Home_Phone_No"))), mismatches)
                sb_eq(f"{p}.Patient_Primary_Email", db_patient.get("Patient_Primary_Email"), parse_str(ocr_value(expected_patient.get("Patient_Primary_Email"))), mismatches)
                sb_eq(f"{p}.Insured_Name", db_patient.get("Insured_Name"), parse_str(ocr_value(expected_patient.get("Insured_Name"))), mismatches)
        elif db_patient is not None:
            sb_add_mismatch(f"{prefix}.Patient present in DB but missing in RawJson.", mismatches)

        expected_payers = claim_obj.get("Payer") or []
        db_payers = fetch_rows_by_fk(conn, tables["payer"], "ClaimRecordId", db_claim["Id"], order_by="Id")
        if len(db_payers) != len(expected_payers):
            sb_add_mismatch(f"{prefix}.Payer count mismatch. expected={len(expected_payers)}, actual={len(db_payers)}", mismatches)
        for p_idx, (db_payer, payer_obj) in enumerate(zip(db_payers, expected_payers)):
            pp = f"{prefix}.Payer[{p_idx}]"
            sb_eq(f"{pp}.Payer_Name", db_payer.get("Payer_Name"), parse_str(ocr_value((payer_obj or {}).get("Payer_Name"))), mismatches)
            sb_eq(f"{pp}.Payer_Id", db_payer.get("Payer_Id"), parse_str(ocr_value((payer_obj or {}).get("Payer_Id"))), mismatches)
            sb_eq(f"{pp}.Payer_Addr1", db_payer.get("Payer_Addr1"), parse_str(ocr_value((payer_obj or {}).get("Payer_Addr1"))), mismatches)
            sb_eq(f"{pp}.Payer_Addr2", db_payer.get("Payer_Addr2"), parse_str(ocr_value((payer_obj or {}).get("Payer_Addr2"))), mismatches)
            sb_eq(f"{pp}.Payer_City", db_payer.get("Payer_City"), parse_str(ocr_value((payer_obj or {}).get("Payer_City"))), mismatches)
            sb_eq(f"{pp}.Payer_State", db_payer.get("Payer_State"), parse_str(ocr_value((payer_obj or {}).get("Payer_State"))), mismatches)
            sb_eq(f"{pp}.Payer_Zip", db_payer.get("Payer_Zip"), parse_str(ocr_value((payer_obj or {}).get("Payer_Zip"))), mismatches)
            sb_eq(f"{pp}.Payer_Type", db_payer.get("Payer_Type"), parse_str(ocr_value((payer_obj or {}).get("Payer_Type"))), mismatches)

        expected_provider_items: list[tuple[str, dict[str, object], bool]] = []
        provider_roles = [
            ("BillingProvider", "BillingProvider", False),
            ("ServicingProvider", "ServicingProvider", False),
            ("ReferringProvider", "ReferringProvider", False),
            ("OrderingProvider", "OrderingProvider", False),
            ("SupervisingProvider", "SupervisingProvider", False),
            ("ServiceFacilityProvider", "ServicingFacility", True),
        ]
        for role_name, json_key, is_facility in provider_roles:
            obj = claim_obj.get(json_key)
            if isinstance(obj, dict):
                expected_provider_items.append((role_name, obj, is_facility))

        db_providers = fetch_rows_by_fk(conn, tables["provider"], "ClaimRecordId", db_claim["Id"], order_by="Id")
        if len(db_providers) != len(expected_provider_items):
            sb_add_mismatch(f"{prefix}.Provider count mismatch. expected={len(expected_provider_items)}, actual={len(db_providers)}", mismatches)

        db_by_role: dict[str, list[dict[str, object]]] = {}
        for row in db_providers:
            role = normalize_enum(row.get("Role")) or "unknown"
            db_by_role.setdefault(role, []).append(row)

        exp_by_role: dict[str, list[tuple[dict[str, object], bool]]] = {}
        for role, obj, is_facility in expected_provider_items:
            exp_by_role.setdefault(role, []).append((obj, is_facility))

        for role, exp_list in exp_by_role.items():
            db_list = db_by_role.get(role, [])
            if len(db_list) != len(exp_list):
                sb_add_mismatch(f"{prefix}.Provider role={role} count mismatch. expected={len(exp_list)}, actual={len(db_list)}", mismatches)
            for r_idx, (db_row, (exp_obj, is_facility)) in enumerate(zip(db_list, exp_list)):
                rp = f"{prefix}.Provider[{role}][{r_idx}]"
                if is_facility:
                    sb_eq(f"{rp}.Provider_Name", db_row.get("Provider_Name"), parse_str(ocr_value(exp_obj.get("Facility_Name"))), mismatches)
                    sb_eq(f"{rp}.Provider_NPI", db_row.get("Provider_NPI"), parse_str(ocr_value(exp_obj.get("Facility_NPI"))), mismatches)
                    sb_eq(f"{rp}.Provider_Addr1", db_row.get("Provider_Addr1"), parse_str(ocr_value(exp_obj.get("Facility_Addr1"))), mismatches)
                    sb_eq(f"{rp}.Provider_Addr2", db_row.get("Provider_Addr2"), parse_str(ocr_value(exp_obj.get("Facility_Addr2"))), mismatches)
                    sb_eq(f"{rp}.Provider_City", db_row.get("Provider_City"), parse_str(ocr_value(exp_obj.get("Provider_City"))), mismatches)
                    sb_eq(f"{rp}.Provider_State", db_row.get("Provider_State"), parse_str(ocr_value(exp_obj.get("Facility_State"))), mismatches)
                    sb_eq(f"{rp}.Provider_Zip", db_row.get("Provider_Zip"), parse_str(ocr_value(exp_obj.get("Facility_Zip"))), mismatches)
                    sb_eq(f"{rp}.Provider_FedId", db_row.get("Provider_FedId"), parse_str(ocr_value(exp_obj.get("Facility_FedId"))), mismatches)
                    sb_eq(f"{rp}.Provider_TaxId", db_row.get("Provider_TaxId"), parse_str(ocr_value(exp_obj.get("Facility_TaxId"))), mismatches)
                    sb_eq(f"{rp}.Provider_Taxonomy", db_row.get("Provider_Taxonomy"), parse_str(ocr_value(exp_obj.get("Facility_Taxonomy"))), mismatches)
                else:
                    sb_eq(f"{rp}.Provider_Name", db_row.get("Provider_Name"), parse_str(ocr_value(exp_obj.get("Provider_Name"))), mismatches)
                    sb_eq(f"{rp}.Provider_NPI", db_row.get("Provider_NPI"), parse_str(ocr_value(exp_obj.get("Provider_NPI"))), mismatches)
                    sb_eq(f"{rp}.Provider_Addr1", db_row.get("Provider_Addr1"), parse_str(ocr_value(exp_obj.get("Provider_Addr1"))), mismatches)
                    sb_eq(f"{rp}.Provider_Addr2", db_row.get("Provider_Addr2"), parse_str(ocr_value(exp_obj.get("Provider_Addr2"))), mismatches)
                    sb_eq(f"{rp}.Provider_City", db_row.get("Provider_City"), parse_str(ocr_value(exp_obj.get("Provider_City"))), mismatches)
                    sb_eq(f"{rp}.Provider_State", db_row.get("Provider_State"), parse_str(ocr_value(exp_obj.get("Provider_State"))), mismatches)
                    sb_eq(f"{rp}.Provider_Zip", db_row.get("Provider_Zip"), parse_str(ocr_value(exp_obj.get("Provider_Zip"))), mismatches)
                    sb_eq(f"{rp}.Provider_FedId", db_row.get("Provider_FedId"), parse_str(ocr_value(exp_obj.get("Provider_FedId"))), mismatches)
                    sb_eq(f"{rp}.Provider_TaxId", db_row.get("Provider_TaxId"), parse_str(ocr_value(exp_obj.get("Provider_TaxId"))), mismatches)
                    sb_eq(f"{rp}.Provider_Taxonomy", db_row.get("Provider_Taxonomy"), parse_str(ocr_value(exp_obj.get("Provider_Taxonomy"))), mismatches)

        expected_dx = claim_obj.get("ClaimDiagnosisCodes")
        db_dx = fetch_rows_by_fk(conn, tables["dx"], "ClaimRecordId", db_claim["Id"], order_by="Id")
        if isinstance(expected_dx, dict):
            if len(db_dx) == 0:
                sb_add_mismatch(f"{prefix}.Diagnosis missing in DB (expected present).", mismatches)
            else:
                dx_row = db_dx[0]
                dp = f"{prefix}.Diagnosis"
                sb_eq(f"{dp}.Primary_DX", dx_row.get("Primary_DX"), parse_str(ocr_value(expected_dx.get("Primary_DX"))), mismatches)
                for i in range(1, 13):
                    key = f"Secondary_DX{i}"
                    sb_eq(f"{dp}.{key}", dx_row.get(key), parse_str(ocr_value(expected_dx.get(key))), mismatches)
        elif len(db_dx) > 0:
            sb_add_mismatch(f"{prefix}.Diagnosis present in DB but missing in RawJson.", mismatches)

        expected_sls = claim_obj.get("ServiceLines") or []
        db_sls = fetch_rows_by_fk(conn, tables["sl"], "ClaimRecordId", db_claim["Id"], order_by="Id")
        if len(db_sls) != len(expected_sls):
            sb_add_mismatch(f"{prefix}.ServiceLines count mismatch. expected={len(expected_sls)}, actual={len(db_sls)}", mismatches)
        for s_idx, (db_sl, sl_obj) in enumerate(zip(db_sls, expected_sls)):
            sp = f"{prefix}.ServiceLines[{s_idx}]"
            sb_eq(f"{sp}.Service_From_Date", to_date_db(db_sl.get("Service_From_Date")), parse_date(ocr_value((sl_obj or {}).get("Service_From_Date"))), mismatches)
            sb_eq(f"{sp}.Service_To_Date", to_date_db(db_sl.get("Service_To_Date")), parse_date(ocr_value((sl_obj or {}).get("Service_To_Date"))), mismatches)
            sb_eq(f"{sp}.Procedure_Code", db_sl.get("Procedure_Code"), parse_str(ocr_value((sl_obj or {}).get("Procedure_Code"))), mismatches)
            sb_eq(f"{sp}.Mod1", db_sl.get("Mod1"), parse_str(ocr_value((sl_obj or {}).get("Mod1"))), mismatches)
            sb_eq(f"{sp}.Mod2", db_sl.get("Mod2"), parse_str(ocr_value((sl_obj or {}).get("Mod2"))), mismatches)
            sb_eq(f"{sp}.Mod3", db_sl.get("Mod3"), parse_str(ocr_value((sl_obj or {}).get("Mod3"))), mismatches)
            sb_eq(f"{sp}.Mod4", db_sl.get("Mod4"), parse_str(ocr_value((sl_obj or {}).get("Mod4"))), mismatches)
            sb_eq(f"{sp}.Service_Billed_Amt", to_decimal_db(db_sl.get("Service_Billed_Amt")), parse_money(ocr_value((sl_obj or {}).get("Service_Billed_Amt"))), mismatches)
            sb_eq(f"{sp}.D_U", db_sl.get("D_U"), parse_int(ocr_value_any((sl_obj or {}), "D_U", "D/U", "DU")), mismatches)
            sb_eq(f"{sp}.Place_Of_Service", db_sl.get("Place_Of_Service"), parse_str(ocr_value((sl_obj or {}).get("Place_Of_Service"))), mismatches)
            sb_eq(f"{sp}.DX_1", db_sl.get("DX_1"), parse_str(ocr_value((sl_obj or {}).get("DX_1"))), mismatches)
            sb_eq(f"{sp}.DX_2", db_sl.get("DX_2"), parse_str(ocr_value((sl_obj or {}).get("DX_2"))), mismatches)
            sb_eq(f"{sp}.DX_3", db_sl.get("DX_3"), parse_str(ocr_value((sl_obj or {}).get("DX_3"))), mismatches)
            sb_eq(f"{sp}.DX_4", db_sl.get("DX_4"), parse_str(ocr_value((sl_obj or {}).get("DX_4"))), mismatches)
            sb_eq(f"{sp}.User_Status", db_sl.get("User_Status"), parse_str(ocr_value((sl_obj or {}).get("User_Status"))), mismatches)

    _SB_ACTIVE_STATS.value = None
    return mismatches, stats


def run_superbill(row_limit: int | None, page_size: int) -> tuple[AuditSummary, list[dict[str, object]]]:
    cfg = resolve_db_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        cursorclass=DictCursor,
        autocommit=True,
    )
    with conn:
        tables = {
            "allocation": resolve_table_name(conn, ["AllocationRecord", "SuperBillAllocation", "SuperBillAllocations"]),
            "claim": resolve_table_name(conn, ["ClaimRecord", "SuperBillClaim", "SuperBillClaims"]),
            "payer": resolve_table_name(conn, ["PayerRecord", "SuperBillPayer", "SuperBillPayers"]),
            "provider": resolve_table_name(conn, ["ClaimProviderRecord", "SuperBillClaimProvider", "SuperBillClaimProviders"]),
            "patient": resolve_table_name(conn, ["PatientRecord", "PatientRecords"]),
            "dx": resolve_table_name(conn, ["DiagnosisCodesRecord", "SuperBillDiagnosisCode", "SuperBillDiagnosisCodes"]),
            "sl": resolve_table_name(conn, ["ServiceLineRecord", "SuperBillServiceLine", "SuperBillServiceLines"]),
        }
        db_max_allocation_id = _fetch_max_allocation_id(conn, tables["allocation"], "RawJson")
        if db_max_allocation_id is not None:
            _log(f"[Superbill] DB max allocation_id with RawJson={db_max_allocation_id}")
        processed_ids, last_allocation_id = _load_langfuse_dataset_state(SUPERBILL_DATASET_NAME)
        if last_allocation_id is not None and db_max_allocation_id is not None and last_allocation_id > db_max_allocation_id:
            _log(
                f"[Superbill] Ignoring outlier Langfuse checkpoint allocation_id={last_allocation_id} "
                f"(DB max={db_max_allocation_id}). Restarting from beginning."
            )
            last_allocation_id = None
        if last_allocation_id is not None:
            last_saved_id = last_allocation_id
            start_from = max(0, last_saved_id - RESUME_LOOKBACK_ALLOCATIONS)
            last_allocation_id = start_from if start_from > 0 else None
            _log(
                f"[Superbill] Starting near checkpoint allocation_id={start_from} "
                f"(last_saved={last_saved_id}, lookback={RESUME_LOOKBACK_ALLOCATIONS})"
            )
        else:
            _log("[Superbill] No prior Langfuse state found; starting from allocation_id=0")

        total_matches = 0
        total_mismatches = 0
        output_rows: list[dict[str, object]] = []
        pending_upload_rows: list[dict[str, object]] = []
        batch_number = 0
        processed_total = 0
        interrupted = False
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS) if MAX_WORKERS > 1 else None
        try:
            while True:
                try:
                    if row_limit is not None and processed_total >= row_limit:
                        _log(f"[Superbill] Reached row limit {row_limit}; stopping.")
                        break
                    batch_number += 1
                    fetch_size = page_size if row_limit is None else min(page_size, row_limit - processed_total)
                    fetched_rows = _fetch_allocations_after_id(
                        conn=conn,
                        table=tables["allocation"],
                        last_allocation_id=last_allocation_id,
                        max_rows=fetch_size,
                        raw_json_column="RawJson",
                    )
                    if not fetched_rows:
                        _log(f"[Superbill] Batch {batch_number}: no new rows found after allocation_id={last_allocation_id or 0}")
                        break

                    last_allocation_id = max(_allocation_sort_key(row) for row in fetched_rows)
                    rows = [
                        row
                        for row in fetched_rows
                        if not _is_allocation_already_uploaded(
                            processed_item_ids=processed_ids,
                            dataset_name=SUPERBILL_DATASET_NAME,
                            source_name="Superbill",
                            allocation_id=row.get("Id"),
                        )
                    ]
                    if not rows:
                        _log(
                            f"[Superbill] Batch {batch_number}: fetched {len(fetched_rows)} rows but all were already uploaded; "
                            f"advanced watermark to allocation_id={last_allocation_id}"
                        )
                        continue

                    _log(
                        f"[Superbill] Batch {batch_number}: processing {len(rows)} rows "
                        f"(allocation_id {_allocation_sort_key(rows[0])} -> {_allocation_sort_key(rows[-1])})"
                    )

                    if executor is not None and len(rows) > 1:
                        futures = [executor.submit(_process_superbill_row_parallel, row, cfg) for row in rows]
                        batch_results: list[tuple[int, CompareStats, dict[str, object], float]] = []
                        for future in as_completed(futures):
                            batch_results.append(future.result())
                        batch_results.sort(key=lambda item: item[0])

                        for row_id, stats, output_row, elapsed in batch_results:
                            total_matches += stats.matches
                            total_mismatches += stats.mismatches
                            output_rows.append(output_row)
                            pending_upload_rows.append(output_row)
                            processed_ids.add(_legacy_dataset_item_id("Superbill", row_id))
                            processed_ids.add(_stable_dataset_item_id(SUPERBILL_DATASET_NAME, "Superbill", row_id))
                            processed_total += 1
                            _log(f"[Superbill] Batch {batch_number}: finished allocation_id={row_id} in {elapsed:.2f}s")
                    else:
                        for row in rows:
                            row_id = row.get("Id")
                            row_started = time.perf_counter()
                            _log(f"[Superbill] Batch {batch_number}: auditing allocation_id={row_id}")
                            _, stats = audit_superbill_allocation(row, conn, tables)
                            output_row = _build_superbill_output_row(row, stats)

                            total_matches += stats.matches
                            total_mismatches += stats.mismatches
                            output_rows.append(output_row)
                            pending_upload_rows.append(output_row)
                            processed_ids.add(_legacy_dataset_item_id("Superbill", row_id))
                            processed_ids.add(_stable_dataset_item_id(SUPERBILL_DATASET_NAME, "Superbill", row_id))
                            processed_total += 1
                            _log(f"[Superbill] Batch {batch_number}: finished allocation_id={row_id} in {time.perf_counter() - row_started:.2f}s")

                    _log(f"[Superbill] Batch {batch_number}: advanced watermark to allocation_id={last_allocation_id}")
                    if UPLOAD_EACH_BATCH and len(pending_upload_rows) >= UPLOAD_EVERY_ROWS:
                        to_upload = list(pending_upload_rows)
                        pending_upload_rows.clear()
                        _log(f"[Superbill] Uploading chunk of {len(to_upload)} rows to Langfuse.")
                        _upload_rows_to_langfuse(to_upload, SUPERBILL_DATASET_NAME, "Superbill")
                except KeyboardInterrupt:
                    interrupted = True
                    if UPLOAD_EACH_BATCH and pending_upload_rows:
                        _log(f"[Superbill] Uploading pending chunk ({len(pending_upload_rows)} rows) before exit.")
                        _upload_rows_to_langfuse(pending_upload_rows, SUPERBILL_DATASET_NAME, "Superbill")
                        pending_upload_rows.clear()
                    _log("[Superbill] Interrupted by user; stopping audit and returning processed rows for upload.")
                    break
        finally:
            if executor is not None:
                executor.shutdown(wait=True)
                _close_superbill_worker_connections()

        if UPLOAD_EACH_BATCH and pending_upload_rows:
            _log(f"[Superbill] Uploading final chunk of {len(pending_upload_rows)} rows to Langfuse.")
            _upload_rows_to_langfuse(pending_upload_rows, SUPERBILL_DATASET_NAME, "Superbill")
            pending_upload_rows.clear()

        if interrupted:
            _log(f"[Superbill] Partial run captured {len(output_rows)} rows before interruption.")

        return (
            AuditSummary(
                name="Superbill",
                audited_allocations=len(output_rows),
                total_matches=total_matches,
                total_mismatches=total_mismatches,
            ),
            output_rows,
        )


def main() -> int:
    load_dotenv_file()
    _log(f"[Config] ENV={LANGFUSE_ENVIRONMENT}, Superbill dataset={SUPERBILL_DATASET_NAME}")
    _log(f"[Config] FILE_TYPE=superbill, MAX_ROWS={MAX_ROWS}, PAGE_SIZE={PAGE_SIZE}")

    try:
        row_limit = _resolve_row_limit(MAX_ROWS)
    except Exception as ex:
        print(f"Superbill run error: {ex}", file=sys.stderr)
        return 1

    try:
        summary, _ = run_superbill(row_limit=row_limit, page_size=PAGE_SIZE)
    except Exception as ex:
        print(f"Superbill run error: {ex}", file=sys.stderr)
        return 1

    print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())