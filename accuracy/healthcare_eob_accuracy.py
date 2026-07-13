#!/usr/bin/env python3
"""Standalone EOB healthcare accuracy runner."""

from __future__ import annotations

import sys
import time
from typing import Optional

import pymysql
from pymysql.cursors import DictCursor

from accuracy.healthcare_accuracy import AuditSummary, CompareStats, EOB_DATASET_NAME, LANGFUSE_ENVIRONMENT
from accuracy.healthcare_accuracy import MAX_ROWS, PAGE_SIZE, RESUME_LOOKBACK_ALLOCATIONS, UPLOAD_EACH_BATCH, UPLOAD_EVERY_ROWS
from accuracy.healthcare_accuracy import _allocation_sort_key, _extract_date_string, _fetch_allocations_after_id
from accuracy.healthcare_accuracy import _fetch_max_allocation_id, _is_allocation_already_uploaded, _legacy_dataset_item_id
from accuracy.healthcare_accuracy import _load_langfuse_dataset_state, _log, _resolve_row_limit, _stable_dataset_item_id
from accuracy.healthcare_accuracy import _to_iso_datetime_string, _upload_rows_to_langfuse, fetch_row_by_id, fetch_rows_by_fk
from accuracy.healthcare_accuracy import load_dotenv_file, map_file_status, normalize_for_compare, normalize_status
from accuracy.healthcare_accuracy import ocr_value, parse_date, parse_int, parse_money, parse_str, print_summary
from accuracy.healthcare_accuracy import resolve_db_config, resolve_table_name, to_date_db, to_decimal_db, value_str


_EOB_ACTIVE_STATS: Optional[CompareStats] = None


def eob_add_mismatch(message: str, mismatches: list[str]) -> None:
    global _EOB_ACTIVE_STATS
    mismatches.append(message)
    if _EOB_ACTIVE_STATS is not None:
        _EOB_ACTIVE_STATS.mismatches += 1


def eob_eq(path: str, actual: object, expected: object, mismatches: list[str]) -> None:
    global _EOB_ACTIVE_STATS
    if normalize_for_compare(actual) == normalize_for_compare(expected):
        if _EOB_ACTIVE_STATS is not None:
            _EOB_ACTIVE_STATS.matches += 1
        return
    eob_add_mismatch(f"{path} mismatch. expected={value_str(expected)}, actual={value_str(actual)}", mismatches)


def audit_eob_allocation(
    allocation_row: dict[str, object],
    conn: pymysql.connections.Connection,
    tables: dict[str, str],
) -> list[str]:
    mismatches: list[str] = []
    raw_json = allocation_row.get("rawJson") or allocation_row.get("RawJson")
    if not raw_json:
        eob_add_mismatch("rawJson missing on allocation row.", mismatches)
        return mismatches

    try:
        import json

        root = json.loads(raw_json)
    except Exception as ex:
        eob_add_mismatch(f"rawJson parse error: {ex}", mismatches)
        return mismatches

    allocation = root.get("Allocation")
    if not isinstance(allocation, dict):
        eob_add_mismatch("rawJson does not contain Allocation object.", mismatches)
        return mismatches

    eob_eq("Allocation.File_name", allocation_row.get("File_name"), allocation.get("File_name"), mismatches)
    eob_eq("Allocation.File_url", allocation_row.get("File_url"), allocation.get("File_url"), mismatches)
    eob_eq("Allocation.Client", allocation_row.get("Client"), allocation.get("Client"), mismatches)
    eob_eq("Allocation.Account", allocation_row.get("Account"), allocation.get("Account"), mismatches)
    eob_eq("Allocation.Payer", allocation_row.get("Payer"), parse_str(ocr_value(allocation.get("Payer"))), mismatches)
    eob_eq("Allocation.Total_No_Of_Clm_On_File", allocation_row.get("Total_No_Of_Clm_On_File"), allocation.get("Total_No_Of_Clm_On_File"), mismatches)
    eob_eq("Allocation.Total_Paid_Amt_On_File", to_decimal_db(allocation_row.get("Total_Paid_Amt_On_File")), parse_money(allocation.get("Total_Paid_Amt_On_File")), mismatches)
    eob_eq("Allocation.Check_Date", to_date_db(allocation_row.get("Check_Date")), parse_date(allocation.get("Check_Date")), mismatches)
    eob_eq("Allocation.Total_Denied_Claims", allocation_row.get("Total_Denied_Claims"), allocation.get("Total_Denied_Claims"), mismatches)
    eob_eq("Allocation.Total_Denied_Lines", allocation_row.get("Total_Denied_Lines"), allocation.get("Total_Denied_Lines"), mismatches)
    eob_eq("Allocation.Total_Posted_Claims", allocation_row.get("Total_Posted_Claims"), parse_int(allocation.get("Total_Posted_Claims")), mismatches)
    eob_eq("Allocation.Download_Date", to_date_db(allocation_row.get("Download_Date")), parse_date(allocation.get("Download_Date")), mismatches)
    eob_eq("Allocation.Completed_Date", to_date_db(allocation_row.get("Completed_Date")), parse_date(allocation.get("Completed_Date")), mismatches)
    eob_eq("Allocation.Not_Completed_Reasons", allocation_row.get("Not_Completed_Reasons"), allocation.get("Not_Completed_Reasons"), mismatches)
    eob_eq("Allocation.File_Status", normalize_status(allocation_row.get("File_Status")), map_file_status(allocation.get("File_Status")), mismatches)

    expected_claims = []
    for claim_info in allocation.get("Claims_Info") or []:
        if isinstance(claim_info, dict) and isinstance(claim_info.get("Claim"), dict):
            expected_claims.append(claim_info["Claim"])

    db_claims = fetch_rows_by_fk(conn, tables["claim"], "AllocationId", allocation_row["Id"], order_by="Id")
    if len(db_claims) != len(expected_claims):
        eob_add_mismatch(f"Claims count mismatch. expected={len(expected_claims)}, actual={len(db_claims)}", mismatches)

    for idx, (db_claim, claim_obj) in enumerate(zip(db_claims, expected_claims)):
        p = f"Claim[{idx}]"
        eob_eq(f"{p}.Claim_Number", db_claim.get("Claim_Number"), parse_str(ocr_value(claim_obj.get("Claim_Number"))), mismatches)
        eob_eq(f"{p}.Claim_Total_Charge_Amt", to_decimal_db(db_claim.get("Claim_Total_Charge_Amt")), parse_money(ocr_value(claim_obj.get("Claim_Total_Charge_Amt"))), mismatches)
        eob_eq(f"{p}.Claim_Paid_Amt", to_decimal_db(db_claim.get("Claim_Paid_Amt")), parse_money(ocr_value(claim_obj.get("Claim_Paid_Amt"))), mismatches)
        eob_eq(f"{p}.Claim_Status_Code", db_claim.get("Claim_Status_Code"), parse_str(ocr_value(claim_obj.get("Claim_Status_Code"))), mismatches)
        eob_eq(f"{p}.Claim_Status_Reason", db_claim.get("Claim_Status_Reason"), parse_str(ocr_value(claim_obj.get("Claim_Status_Reason"))), mismatches)
        eob_eq(f"{p}.Claim_Facility_Type", db_claim.get("Claim_Facility_Type"), parse_str(ocr_value(claim_obj.get("Claim_Facility_Type"))), mismatches)
        eob_eq(f"{p}.Claim_Frequency", db_claim.get("Claim_Frequency"), parse_str(ocr_value(claim_obj.get("Claim_Frequency"))), mismatches)
        eob_eq(f"{p}.Claim_Date_of_Service_From", to_date_db(db_claim.get("Claim_Date_of_Service_From")), parse_date(ocr_value(claim_obj.get("Claim_Date_of_Service_From"))), mismatches)
        eob_eq(f"{p}.Claim_Date_of_Service_To", to_date_db(db_claim.get("Claim_Date_of_Service_To")), parse_date(ocr_value(claim_obj.get("Claim_Date_of_Service_To"))), mismatches)
        eob_eq(f"{p}.Claim_Recieved_Date", to_date_db(db_claim.get("Claim_Recieved_Date")), parse_date(ocr_value(claim_obj.get("Claim_Recieved_Date"))), mismatches)
        eob_eq(f"{p}.Patient_Responsibility_Amt", to_decimal_db(db_claim.get("Patient_Responsibility_Amt")), parse_money(ocr_value(claim_obj.get("Patient_Responsibility_Amt"))), mismatches)
        eob_eq(f"{p}.CLIA_Number", db_claim.get("CLIA_Number"), parse_str(ocr_value(claim_obj.get("CLIA_Number"))), mismatches)
        eob_eq(f"{p}.Claim_Invoice_Number", db_claim.get("Claim_Invoice_Number"), parse_str(ocr_value(claim_obj.get("Claim_Invoice_Number"))), mismatches)
        eob_eq(f"{p}.Admission_Date", to_date_db(db_claim.get("Admission_Date")), parse_date(ocr_value(claim_obj.get("Admission_Date"))), mismatches)
        eob_eq(f"{p}.Patient_Last_Seen_Date", to_date_db(db_claim.get("Patient_Last_Seen_Date")), parse_date(ocr_value(claim_obj.get("Patient_Last_Seen_Date"))), mismatches)

        expected_payer = claim_obj.get("Payer")
        db_payer = fetch_row_by_id(conn, tables["payer"], db_claim.get("PayerId")) if db_claim.get("PayerId") else None
        if isinstance(expected_payer, dict):
            if db_payer is None:
                eob_add_mismatch(f"{p}.Payer missing in DB (expected present).", mismatches)
            else:
                pp = f"{p}.Payer"
                eob_eq(f"{pp}.Payer_Name", db_payer.get("Payer_Name"), parse_str(ocr_value(expected_payer.get("Payer_Name"))), mismatches)
                eob_eq(f"{pp}.Payer_Id", db_payer.get("Payer_Id"), parse_str(ocr_value(expected_payer.get("Payer_Id"))), mismatches)
                eob_eq(f"{pp}.Payer_Addr1", db_payer.get("Payer_Addr1"), parse_str(ocr_value(expected_payer.get("Payer_Addr1"))), mismatches)
                eob_eq(f"{pp}.Payer_Addr2", db_payer.get("Payer_Addr2"), parse_str(ocr_value(expected_payer.get("Payer_Addr2"))), mismatches)
                eob_eq(f"{pp}.Payer_City", db_payer.get("Payer_City"), parse_str(ocr_value(expected_payer.get("Payer_City"))), mismatches)
                eob_eq(f"{pp}.Payer_State", db_payer.get("Payer_State"), parse_str(ocr_value(expected_payer.get("Payer_State"))), mismatches)
                eob_eq(f"{pp}.Payer_Zip", db_payer.get("Payer_Zip"), parse_str(ocr_value(expected_payer.get("Payer_Zip"))), mismatches)
        elif db_payer is not None:
            eob_add_mismatch(f"{p}.Payer present in DB but missing in rawJson.", mismatches)

        expected_payee = claim_obj.get("Payee")
        db_payee = fetch_row_by_id(conn, tables["payee"], db_claim.get("PayeeId")) if db_claim.get("PayeeId") else None
        if isinstance(expected_payee, dict):
            if db_payee is None:
                eob_add_mismatch(f"{p}.Payee missing in DB (expected present).", mismatches)
            else:
                pe = f"{p}.Payee"
                eob_eq(f"{pe}.Payee_Name", db_payee.get("Payee_Name"), parse_str(ocr_value(expected_payee.get("Payee_Name"))), mismatches)
                eob_eq(f"{pe}.Payee_NPI", db_payee.get("Payee_NPI"), parse_str(ocr_value(expected_payee.get("Payee_NPI"))), mismatches)
                eob_eq(f"{pe}.Payee_TaxID", db_payee.get("Payee_TaxID"), parse_str(ocr_value(expected_payee.get("Payee_TaxID"))), mismatches)
                eob_eq(f"{pe}.Payee_Addr1", db_payee.get("Payee_Addr1"), parse_str(ocr_value(expected_payee.get("Payee_Addr1"))), mismatches)
                eob_eq(f"{pe}.Payee_Addr2", db_payee.get("Payee_Addr2"), parse_str(ocr_value(expected_payee.get("Payee_Addr2"))), mismatches)
                eob_eq(f"{pe}.Payee_City", db_payee.get("Payee_City"), parse_str(ocr_value(expected_payee.get("Payee_City"))), mismatches)
                eob_eq(f"{pe}.Payee_State", db_payee.get("Payee_State"), parse_str(ocr_value(expected_payee.get("Payee_State"))), mismatches)
                eob_eq(f"{pe}.Payee_Zip", db_payee.get("Payee_Zip"), parse_str(ocr_value(expected_payee.get("Payee_Zip"))), mismatches)
                eob_eq(f"{pe}.Rendering_Provider_Name", db_payee.get("Rendering_Provider_Name"), parse_str(ocr_value(expected_payee.get("Rendering_Provider_Name"))), mismatches)
                eob_eq(f"{pe}.Rendering_Provider_NPI", db_payee.get("Rendering_Provider_NPI"), parse_str(ocr_value(expected_payee.get("Rendering_Provider_NPI"))), mismatches)
                eob_eq(f"{pe}.Check_EFT_Number", db_payee.get("Check_EFT_Number"), parse_str(ocr_value(expected_payee.get("Check_EFT_Number"))), mismatches)
                eob_eq(f"{pe}.Payment_Amt", to_decimal_db(db_payee.get("Payment_Amt")), parse_money(ocr_value(expected_payee.get("Payment_Amt"))), mismatches)
                eob_eq(f"{pe}.Check_Date", to_date_db(db_payee.get("Check_Date")), parse_date(ocr_value(expected_payee.get("Check_Date"))), mismatches)
        elif db_payee is not None:
            eob_add_mismatch(f"{p}.Payee present in DB but missing in rawJson.", mismatches)

        expected_patient = claim_obj.get("Patient")
        db_patient = fetch_row_by_id(conn, tables["patient"], db_claim.get("PatientId")) if db_claim.get("PatientId") else None
        if isinstance(expected_patient, dict):
            if db_patient is None:
                eob_add_mismatch(f"{p}.Patient missing in DB (expected present).", mismatches)
            else:
                pt = f"{p}.Patient"
                eob_eq(f"{pt}.Patient_FN", db_patient.get("Patient_FN"), parse_str(ocr_value(expected_patient.get("Patient_FN"))), mismatches)
                eob_eq(f"{pt}.Patient_LN", db_patient.get("Patient_LN"), parse_str(ocr_value(expected_patient.get("Patient_LN"))), mismatches)
                eob_eq(f"{pt}.Patient_MI", db_patient.get("Patient_MI"), parse_str(ocr_value(expected_patient.get("Patient_MI"))), mismatches)
                eob_eq(f"{pt}.Patient_Id", db_patient.get("Patient_Id"), parse_str(ocr_value(expected_patient.get("Patient_Id"))), mismatches)
                eob_eq(f"{pt}.Patient_Control_Number", db_patient.get("Patient_Control_Number"), parse_str(ocr_value(expected_patient.get("Patient_Control_Number"))), mismatches)
                eob_eq(f"{pt}.Patient_Group", db_patient.get("Patient_Group"), parse_str(ocr_value(expected_patient.get("Patient_Group"))), mismatches)
                eob_eq(f"{pt}.Patient_Addr1", db_patient.get("Patient_Addr1"), parse_str(ocr_value(expected_patient.get("Patient_Addr1"))), mismatches)
                eob_eq(f"{pt}.Patient_Addr2", db_patient.get("Patient_Addr2"), parse_str(ocr_value(expected_patient.get("Patient_Addr2"))), mismatches)
                eob_eq(f"{pt}.Patient_City", db_patient.get("Patient_City"), parse_str(ocr_value(expected_patient.get("Patient_City"))), mismatches)
                eob_eq(f"{pt}.Patient_State", db_patient.get("Patient_State"), parse_str(ocr_value(expected_patient.get("Patient_State"))), mismatches)
                eob_eq(f"{pt}.Patient_Zip", db_patient.get("Patient_Zip"), parse_str(ocr_value(expected_patient.get("Patient_Zip"))), mismatches)
                eob_eq(f"{pt}.Patient_DOB", to_date_db(db_patient.get("Patient_DOB")), parse_date(ocr_value(expected_patient.get("Patient_DOB"))), mismatches)
                eob_eq(f"{pt}.Patient_Gender", db_patient.get("Patient_Gender"), parse_str(ocr_value(expected_patient.get("Patient_Gender"))), mismatches)
                eob_eq(f"{pt}.Patient_Relationship", db_patient.get("Patient_Relationship"), parse_str(ocr_value(expected_patient.get("Patient_Relationship"))), mismatches)
                eob_eq(f"{pt}.Insured_Name", db_patient.get("Insured_Name"), parse_str(ocr_value(expected_patient.get("Insured_Name"))), mismatches)
        elif db_patient is not None:
            eob_add_mismatch(f"{p}.Patient present in DB but missing in rawJson.", mismatches)

        expected_dx = claim_obj.get("Claim_Diagnosis")
        db_dx = fetch_rows_by_fk(conn, tables["diagnosis"], "ClaimId", db_claim["Id"], order_by="Id")
        if isinstance(expected_dx, dict):
            if not db_dx:
                eob_add_mismatch(f"{p}.Claim_Diagnosis missing in DB (expected present).", mismatches)
            else:
                dx = db_dx[0]
                pd = f"{p}.Claim_Diagnosis"
                eob_eq(f"{pd}.Primary_DX", dx.get("Primary_DX"), parse_str(ocr_value(expected_dx.get("Primary_DX"))), mismatches)
                for i in range(1, 13):
                    key = f"Secondary_DX{i}"
                    eob_eq(f"{pd}.{key}", dx.get(key), parse_str(ocr_value(expected_dx.get(key))), mismatches)
        elif db_dx:
            eob_add_mismatch(f"{p}.Claim_Diagnosis present in DB but missing in rawJson.", mismatches)

        expected_sls = claim_obj.get("Service_Line_Items") or []
        db_sls = fetch_rows_by_fk(conn, tables["service_line"], "ClaimId", db_claim["Id"], order_by="Id")
        if len(db_sls) != len(expected_sls):
            eob_add_mismatch(f"{p}.Service_Line_Items count mismatch. expected={len(expected_sls)}, actual={len(db_sls)}", mismatches)

        for sl_idx, (db_sl, sl_obj) in enumerate(zip(db_sls, expected_sls)):
            ps = f"{p}.Service_Line_Items[{sl_idx}]"
            eob_eq(f"{ps}.Service_From_Date", to_date_db(db_sl.get("Service_From_Date")), parse_date(ocr_value(sl_obj.get("Service_From_Date"))), mismatches)
            eob_eq(f"{ps}.Service_To_Date", to_date_db(db_sl.get("Service_To_Date")), parse_date(ocr_value(sl_obj.get("Service_To_Date"))), mismatches)
            eob_eq(f"{ps}.Procedure_Code", db_sl.get("Procedure_Code"), parse_str(ocr_value(sl_obj.get("Procedure_Code"))), mismatches)
            eob_eq(f"{ps}.Mod1", db_sl.get("Mod1"), parse_str(ocr_value(sl_obj.get("Mod1"))), mismatches)
            eob_eq(f"{ps}.Mod2", db_sl.get("Mod2"), parse_str(ocr_value(sl_obj.get("Mod2"))), mismatches)
            eob_eq(f"{ps}.Mod3", db_sl.get("Mod3"), parse_str(ocr_value(sl_obj.get("Mod3"))), mismatches)
            eob_eq(f"{ps}.Mod4", db_sl.get("Mod4"), parse_str(ocr_value(sl_obj.get("Mod4"))), mismatches)
            eob_eq(f"{ps}.Service_Billed_Amt", to_decimal_db(db_sl.get("Service_Billed_Amt")), parse_money(ocr_value(sl_obj.get("Service_Billed_Amt"))), mismatches)
            eob_eq(f"{ps}.Service_Allowed_Amt", to_decimal_db(db_sl.get("Service_Allowed_Amt")), parse_money(ocr_value(sl_obj.get("Service_Allowed_Amt"))), mismatches)
            eob_eq(f"{ps}.Service_Paid_Amt", to_decimal_db(db_sl.get("Service_Paid_Amt")), parse_money(ocr_value(sl_obj.get("Service_Paid_Amt"))), mismatches)
            eob_eq(f"{ps}.D_U", db_sl.get("D_U"), parse_str(ocr_value(sl_obj.get("D_U"))), mismatches)
            eob_eq(f"{ps}.Place_Of_Service", db_sl.get("Place_Of_Service"), parse_str(ocr_value(sl_obj.get("Place_Of_Service"))), mismatches)
            eob_eq(f"{ps}.DX_1", db_sl.get("DX_1"), parse_str(ocr_value(sl_obj.get("DX_1"))), mismatches)
            eob_eq(f"{ps}.DX_2", db_sl.get("DX_2"), parse_str(ocr_value(sl_obj.get("DX_2"))), mismatches)
            eob_eq(f"{ps}.DX_3", db_sl.get("DX_3"), parse_str(ocr_value(sl_obj.get("DX_3"))), mismatches)
            eob_eq(f"{ps}.DX_4", db_sl.get("DX_4"), parse_str(ocr_value(sl_obj.get("DX_4"))), mismatches)
            eob_eq(f"{ps}.User_Status", db_sl.get("User_Status"), parse_str(ocr_value(sl_obj.get("User_Status"))), mismatches)

            expected_adj = sl_obj.get("Service_Adjustments") or []
            db_adj = fetch_rows_by_fk(conn, tables["service_adjustment"], "ServiceLineItemId", db_sl["Id"], order_by="Id")
            if len(db_adj) != len(expected_adj):
                eob_add_mismatch(f"{ps}.Service_Adjustments count mismatch. expected={len(expected_adj)}, actual={len(db_adj)}", mismatches)
            for a_idx, (db_a, a_obj) in enumerate(zip(db_adj, expected_adj)):
                pa = f"{ps}.Service_Adjustments[{a_idx}]"
                eob_eq(f"{pa}.Service_Adjustment_Reason_Code", db_a.get("Service_Adjustment_Reason_Code"), parse_str(ocr_value(a_obj.get("Service_Adjustment_Reason_Code"))), mismatches)
                eob_eq(f"{pa}.Service_Adjustment_Group_Code", db_a.get("Service_Adjustment_Group_Code"), parse_str(ocr_value(a_obj.get("Service_Adjustment_Group_Code"))), mismatches)
                eob_eq(f"{pa}.Service_Adjustment_Reason", db_a.get("Service_Adjustment_Reason"), parse_str(ocr_value(a_obj.get("Service_Adjustment_Reason"))), mismatches)
                eob_eq(f"{pa}.Service_Adjustment_Amount", to_decimal_db(db_a.get("Service_Adjustment_Amount")), parse_money(ocr_value(a_obj.get("Service_Adjustment_Amount"))), mismatches)

    return mismatches


def run_eob(row_limit: int | None, page_size: int) -> tuple[AuditSummary, list[dict[str, object]]]:
    global _EOB_ACTIVE_STATS
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
            "allocation": resolve_table_name(conn, ["EOB_Allocation", "EOBAllocations"]),
            "claim": resolve_table_name(conn, ["EOB_Claim", "EOBClaims"]),
            "payer": resolve_table_name(conn, ["EOB_Payer", "EOBPayers"]),
            "payee": resolve_table_name(conn, ["EOB_Payee", "EOBPayees"]),
            "diagnosis": resolve_table_name(conn, ["EOB_Claim_Diagnosis", "EOBClaim_Diagnosis"]),
            "service_line": resolve_table_name(conn, ["EOB_Service_Line_Item", "EOBService_Line_Items"]),
            "service_adjustment": resolve_table_name(conn, ["EOB_Service_Adjustment", "EOBService_Adjustments"]),
            "patient": resolve_table_name(conn, ["PatientRecord", "PatientRecords"]),
        }
        db_max_allocation_id = _fetch_max_allocation_id(conn, tables["allocation"], "rawJson")
        if db_max_allocation_id is not None:
            _log(f"[EOB] DB max allocation_id with rawJson={db_max_allocation_id}")
        processed_ids, last_allocation_id = _load_langfuse_dataset_state(EOB_DATASET_NAME)
        if last_allocation_id is not None and db_max_allocation_id is not None and last_allocation_id > db_max_allocation_id:
            _log(
                f"[EOB] Ignoring outlier Langfuse checkpoint allocation_id={last_allocation_id} "
                f"(DB max={db_max_allocation_id}). Restarting from beginning."
            )
            last_allocation_id = None
        if last_allocation_id is not None:
            last_saved_id = last_allocation_id
            start_from = max(0, last_saved_id - RESUME_LOOKBACK_ALLOCATIONS)
            last_allocation_id = start_from if start_from > 0 else None
            _log(
                f"[EOB] Starting near checkpoint allocation_id={start_from} "
                f"(last_saved={last_saved_id}, lookback={RESUME_LOOKBACK_ALLOCATIONS})"
            )
        else:
            _log("[EOB] No prior Langfuse state found; starting from allocation_id=0")

        total_matches = 0
        total_mismatches = 0
        output_rows: list[dict[str, object]] = []
        pending_upload_rows: list[dict[str, object]] = []
        batch_number = 0
        processed_total = 0
        interrupted = False

        while True:
            try:
                if row_limit is not None and processed_total >= row_limit:
                    _log(f"[EOB] Reached row limit {row_limit}; stopping.")
                    break
                batch_number += 1
                fetch_size = page_size if row_limit is None else min(page_size, row_limit - processed_total)
                fetched_rows = _fetch_allocations_after_id(
                    conn=conn,
                    table=tables["allocation"],
                    last_allocation_id=last_allocation_id,
                    max_rows=fetch_size,
                    raw_json_column="rawJson",
                )
                if not fetched_rows:
                    _log(f"[EOB] Batch {batch_number}: no new rows found after allocation_id={last_allocation_id or 0}")
                    break

                last_allocation_id = max(_allocation_sort_key(row) for row in fetched_rows)
                rows = [
                    row
                    for row in fetched_rows
                    if not _is_allocation_already_uploaded(
                        processed_item_ids=processed_ids,
                        dataset_name=EOB_DATASET_NAME,
                        source_name="EOB",
                        allocation_id=row.get("Id"),
                    )
                ]
                if not rows:
                    _log(
                        f"[EOB] Batch {batch_number}: fetched {len(fetched_rows)} rows but all were already uploaded; "
                        f"advanced watermark to allocation_id={last_allocation_id}"
                    )
                    continue

                _log(
                    f"[EOB] Batch {batch_number}: processing {len(rows)} rows "
                    f"(allocation_id {_allocation_sort_key(rows[0])} -> {_allocation_sort_key(rows[-1])})"
                )

                for row in rows:
                    row_id = row.get("Id")
                    row_started = time.perf_counter()
                    _log(f"[EOB] Batch {batch_number}: auditing allocation_id={row_id}")
                    _EOB_ACTIVE_STATS = CompareStats()
                    mismatches = audit_eob_allocation(row, conn, tables)
                    stats = _EOB_ACTIVE_STATS or CompareStats()
                    _EOB_ACTIVE_STATS = None

                    total_matches += stats.matches
                    total_mismatches += len(mismatches)
                    allocation_total = stats.matches + len(mismatches)
                    accuracy_pct = (stats.matches / allocation_total * 100.0) if allocation_total else 0.0

                    output_row = {
                        "date": _extract_date_string(row.get("Download_Date") or row.get("download_date")),
                        "accuracy": round(accuracy_pct, 4),
                        "date_time": _to_iso_datetime_string(row.get("UpdatedAt") or row.get("UpdatedOn") or row.get("CreatedOn")),
                        "file_name": row.get("File_name") or row.get("file_name"),
                        "client_name": row.get("Client") or row.get("client_name"),
                        "allocation_id": row.get("Id"),
                        "total_matched": stats.matches,
                        "eob_or_superbill": "EOB",
                        "total_mismatches": len(mismatches),
                        "total_matches": stats.matches,
                        "accuracy_pct": accuracy_pct,
                        "download_date": _to_iso_datetime_string(row.get("Download_Date") or row.get("download_date")),
                        "source_type": "EOB",
                    }
                    output_rows.append(output_row)
                    pending_upload_rows.append(output_row)
                    processed_ids.add(_legacy_dataset_item_id("EOB", row_id))
                    processed_ids.add(_stable_dataset_item_id(EOB_DATASET_NAME, "EOB", row_id))
                    processed_total += 1
                    _log(f"[EOB] Batch {batch_number}: finished allocation_id={row_id} in {time.perf_counter() - row_started:.2f}s")

                _log(f"[EOB] Batch {batch_number}: advanced watermark to allocation_id={last_allocation_id}")
                if UPLOAD_EACH_BATCH and len(pending_upload_rows) >= UPLOAD_EVERY_ROWS:
                    to_upload = list(pending_upload_rows)
                    pending_upload_rows.clear()
                    _log(f"[EOB] Uploading chunk of {len(to_upload)} rows to Langfuse.")
                    _upload_rows_to_langfuse(to_upload, EOB_DATASET_NAME, "EOB")
            except KeyboardInterrupt:
                interrupted = True
                _EOB_ACTIVE_STATS = None
                if UPLOAD_EACH_BATCH and pending_upload_rows:
                    _log(f"[EOB] Uploading pending chunk ({len(pending_upload_rows)} rows) before exit.")
                    _upload_rows_to_langfuse(pending_upload_rows, EOB_DATASET_NAME, "EOB")
                    pending_upload_rows.clear()
                _log("[EOB] Interrupted by user; stopping audit and returning processed rows for upload.")
                break

        if UPLOAD_EACH_BATCH and pending_upload_rows:
            _log(f"[EOB] Uploading final chunk of {len(pending_upload_rows)} rows to Langfuse.")
            _upload_rows_to_langfuse(pending_upload_rows, EOB_DATASET_NAME, "EOB")
            pending_upload_rows.clear()

        if interrupted:
            _log(f"[EOB] Partial run captured {len(output_rows)} rows before interruption.")

        return (
            AuditSummary(
                name="EOB",
                audited_allocations=len(output_rows),
                total_matches=total_matches,
                total_mismatches=total_mismatches,
            ),
            output_rows,
        )


def main() -> int:
    load_dotenv_file()
    _log(f"[Config] ENV={LANGFUSE_ENVIRONMENT}, EOB dataset={EOB_DATASET_NAME}")
    _log(f"[Config] FILE_TYPE=eob, MAX_ROWS={MAX_ROWS}, PAGE_SIZE={PAGE_SIZE}")

    try:
        row_limit = _resolve_row_limit(MAX_ROWS)
    except Exception as ex:
        print(f"EOB run error: {ex}", file=sys.stderr)
        return 1

    try:
        summary, _ = run_eob(row_limit=row_limit, page_size=PAGE_SIZE)
    except Exception as ex:
        print(f"EOB run error: {ex}", file=sys.stderr)
        return 1

    print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())