#!/usr/bin/env python3
"""Check connectivity to IDP SQL Server using the same env vars as accuracy/idp_accuarcy.py."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pyodbc

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def _mask(value: str) -> str:
    if not value:
        return "<missing>"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _load_dotenv_if_available() -> None:
    if load_dotenv is not None:
        load_dotenv(override=False)


def _load_local_settings_if_available() -> None:
    root = Path(__file__).resolve().parents[1]
    settings_path = root / "local.settings.json"
    if not settings_path.exists():
        return

    try:
        with settings_path.open("r", encoding="utf-8") as f:
            values = json.load(f).get("Values", {})
        for key, value in values.items():
            if key not in os.environ and value is not None:
                os.environ[key] = str(value)
    except Exception as ex:
        print(f"[WARN] Could not load local.settings.json: {ex}")


def _resolve_config(args: argparse.Namespace) -> dict[str, str]:
    server = (args.server or os.getenv("IDP_SQL_SERVER") or "").strip()
    database = (args.database or os.getenv("IDP_SQL_DATABASE") or "").strip()
    user = (args.user or os.getenv("IDP_SQL_USER") or "").strip()
    password = (args.password or os.getenv("IDP_SQL_PASSWORD") or "").strip()

    missing = []
    if not server:
        missing.append("IDP_SQL_SERVER")
    if not database:
        missing.append("IDP_SQL_DATABASE")
    if not user:
        missing.append("IDP_SQL_USER")
    if not password:
        missing.append("IDP_SQL_PASSWORD")

    if missing:
        raise ValueError("Missing required settings: " + ", ".join(missing))

    return {
        "server": server,
        "database": database,
        "user": user,
        "password": password,
    }


def _pick_sql_driver() -> str:
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    installed = set(pyodbc.drivers())
    selected = next((d for d in preferred if d in installed), None)
    if not selected:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Install ODBC Driver 18 or 17 for SQL Server."
        )
    return selected


def _build_conn_str(
    server: str,
    database: str,
    user: str,
    password: str,
    driver: str,
    timeout: int,
    trust_server_certificate: bool,
) -> str:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Connect Timeout={timeout};"
    )

    if "ODBC Driver" in driver:
        if trust_server_certificate:
            conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
        else:
            conn_str += "Encrypt=yes;TrustServerCertificate=no;"

    return conn_str


def _check_connection(conn_str: str, check_view: bool) -> None:
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS ok")
        row = cursor.fetchone()
        print(f"[OK] SQL ping result: {row[0] if row else 'unknown'}")

        if check_view:
            cursor.execute("SELECT TOP 1 * FROM dbo.vw_PdfClassificationTransactionLog")
            cursor.fetchone()
            print("[OK] View access confirmed: dbo.vw_PdfClassificationTransactionLog")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check IDP SQL Server connectivity.")
    parser.add_argument("--server", help="Override IDP_SQL_SERVER")
    parser.add_argument("--database", help="Override IDP_SQL_DATABASE")
    parser.add_argument("--user", help="Override IDP_SQL_USER")
    parser.add_argument("--password", help="Override IDP_SQL_PASSWORD")
    parser.add_argument("--timeout", type=int, default=30, help="Connection timeout in seconds")
    parser.add_argument(
        "--trust-server-certificate",
        action="store_true",
        help="Use TrustServerCertificate=yes for encrypted ODBC connections",
    )
    parser.add_argument(
        "--skip-view-check",
        action="store_true",
        help="Skip checking dbo.vw_PdfClassificationTransactionLog",
    )
    return parser.parse_args()


def main() -> int:
    _load_dotenv_if_available()
    _load_local_settings_if_available()

    args = _parse_args()

    try:
        cfg = _resolve_config(args)
        driver = _pick_sql_driver()
    except Exception as ex:
        print(f"[ERROR] {ex}")
        return 1

    print("[INFO] IDP SQL config")
    print(f"       server   : {cfg['server']}")
    print(f"       database : {cfg['database']}")
    print(f"       user     : {_mask(cfg['user'])}")
    print(f"       password : {_mask(cfg['password'])}")
    print(f"       driver   : {driver}")

    conn_str = _build_conn_str(
        server=cfg["server"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        driver=driver,
        timeout=args.timeout,
        trust_server_certificate=args.trust_server_certificate,
    )

    try:
        _check_connection(conn_str, check_view=not args.skip_view_check)
    except Exception as ex:
        print(f"[ERROR] Connection failed: {ex}")
        return 2

    print("[SUCCESS] IDP SQL connectivity check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
