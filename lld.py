#!/usr/bin/env python3
"""
get_db_schema.py

Extracts table/column schema (name, data type, length, precision, nullability,
key info) from one or more of the databases used by this function app:

  - SQL Server   (pymssql)  -> e.g. the SQL Server "Eval Metrics" source
  - MySQL/MariaDB (pymysql) -> e.g. the MySQL "Eval Metrics" / Tabak source

Reads connection info from environment variables (.env), so no credentials
are hard-coded. Add whichever *_DB_* variables apply to your environment;
blocks for sources you don't have configured are skipped automatically.

Usage:
    pip install pymssql pymysql pyodbc python-dotenv --break-system-packages
    python get_db_schema.py                      # prints + writes CSVs
    python get_db_schema.py --format json         # writes JSON instead
    python get_db_schema.py --only idp            # sqlserver | mysql | healthcare_ai | tabak | idp | all

Expected environment variables (set what you have, skip the rest):

    SQLSERVER_HOST, SQLSERVER_PORT (default 1433), SQLSERVER_USER,
    SQLSERVER_PASSWORD, SQLSERVER_DATABASE

    MYSQL_HOST, MYSQL_PORT (default 3306), MYSQL_USER,
    MYSQL_PASSWORD, MYSQL_DATABASE

    HEALTHCARE_AI_DB_SERVER / HEALTHCARE_AI_DB_JDBC_URL, HEALTHCARE_AI_DB_PORT,
    HEALTHCARE_AI_DB_USERID, HEALTHCARE_AI_DB_PASSWORD, HEALTHCARE_AI_DB_DATABASE
    (the actual DB behind eob_fine_tuning_data_get.py / superbill_fine_tuning_data_get.py)

    TABAK_DB_SERVER / TABAK_DB_JDBC_URL, TABAK_DB_PORT, TABAK_DB_USERID,
    TABAK_DB_PASSWORD, TABAK_DB_DATABASE   (already used by tabak_accuarcy.py)

    IDP_SQL_SERVER, IDP_SQL_DATABASE, IDP_SQL_USER, IDP_SQL_PASSWORD
    (used directly in idp_accuarcy.py via pyodbc; requires an ODBC Driver 17/18
    for SQL Server to be installed locally)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def load_local_settings() -> None:
    """Load variables from local.settings.json Values when present."""
    settings_path = Path(__file__).resolve().parent / "local.settings.json"
    if not settings_path.exists():
        return

    try:
        with settings_path.open("r", encoding="utf-8") as f:
            values = json.load(f).get("Values", {})
        for key, value in values.items():
            if key not in os.environ and value is not None:
                os.environ[key] = str(value)
    except Exception as ex:
        print(f"Warning: Could not load local.settings.json: {ex}", file=sys.stderr)


load_local_settings()

OUTPUT_DIR = Path("schema_output")


def env(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v:
            return v.strip()
    return default


# ----------------------------------------------------------------------
# SQL Server
# ----------------------------------------------------------------------
def get_sqlserver_config() -> dict[str, Any] | None:
    host = env("SQLSERVER_HOST", "SQL_SERVER_HOST")
    if not host:
        return None
    return {
        "server": host,
        "port": int(env("SQLSERVER_PORT", default="1433")),
        "user": env("SQLSERVER_USER", "SQL_SERVER_USER"),
        "password": env("SQLSERVER_PASSWORD", "SQL_SERVER_PASSWORD"),
        "database": env("SQLSERVER_DATABASE", "SQL_SERVER_DATABASE"),
    }


def fetch_sqlserver_schema(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    import pymssql

    conn = pymssql.connect(
        server=cfg["server"], port=cfg["port"], user=cfg["user"],
        password=cfg["password"], database=cfg["database"], timeout=15,
    )
    query = """
        SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
              ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk
          ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
         AND c.TABLE_NAME = pk.TABLE_NAME
         AND c.COLUMN_NAME = pk.COLUMN_NAME
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
    """
    with conn.cursor(as_dict=True) as cur:
        cur.execute(query)
        rows = list(cur)
    conn.close()
    for r in rows:
        r["SOURCE"] = "sqlserver"
    return rows


# ----------------------------------------------------------------------
# MySQL / MariaDB / IDP
# ----------------------------------------------------------------------
def get_idp_config() -> dict[str, Any] | None:
    """Mirrors the connection vars used directly in idp_accuarcy.py (pyodbc, not pymssql)."""
    server = env("IDP_SQL_SERVER")
    database = env("IDP_SQL_DATABASE")
    if not server or not database:
        return None
    return {
        "server": server,
        "database": database,
        "user": env("IDP_SQL_USER"),
        "password": env("IDP_SQL_PASSWORD", default=""),
    }


def fetch_idp_schema(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Connects the same way idp_accuarcy.py does: pyodbc with driver auto-detection."""
    import pyodbc

    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
    installed_drivers = set(pyodbc.drivers())
    selected_driver = next((d for d in preferred_drivers if d in installed_drivers), None)
    if not selected_driver:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Install ODBC Driver 18 or 17 for SQL Server."
        )

    conn_str = (
        f"DRIVER={{{selected_driver}}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['user']};"
        f"PWD={cfg['password']};"
        "Connect Timeout=30;"
    )
    if "ODBC Driver" in selected_driver:
        conn_str += "Encrypt=yes;TrustServerCertificate=no;"

    conn = pyodbc.connect(conn_str)
    query = """
        SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
              ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk
          ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
         AND c.TABLE_NAME = pk.TABLE_NAME
         AND c.COLUMN_NAME = pk.COLUMN_NAME
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    for r in rows:
        r["SOURCE"] = "idp"
    return rows


def get_mysql_config(prefix: str = "MYSQL") -> dict[str, Any] | None:
    host = env(f"{prefix}_HOST")
    database = env(f"{prefix}_DATABASE")
    if not host or not database:
        return None
    return {
        "host": host,
        "port": int(env(f"{prefix}_PORT", default="3306")),
        "user": env(f"{prefix}_USER", f"{prefix}_USERID"),
        "password": env(f"{prefix}_PASSWORD"),
        "database": database,
    }


def get_healthcare_ai_config() -> dict[str, Any] | None:
    """Mirrors resolve_db_config() from eob_fine_tuning_data_get.py / superbill_fine_tuning_data_get.py."""
    host = env("HEALTHCARE_AI_DB_SERVER")
    port_raw = env("HEALTHCARE_AI_DB_PORT")
    user = env("HEALTHCARE_AI_DB_USERID")
    password = env("HEALTHCARE_AI_DB_PASSWORD")
    database = env("HEALTHCARE_AI_DB_DATABASE")

    jdbc = env("HEALTHCARE_AI_DB_JDBC_URL")
    if jdbc:
        m = re.match(r"^jdbc:mysql://([^/:?#]+)(?::(\d+))?/([^?]+)", jdbc.strip(), re.IGNORECASE)
        if m:
            host = host or m.group(1)
            if not port_raw and m.group(2):
                port_raw = m.group(2)
            database = database or m.group(3)

    if not host or not database:
        return None
    return {
        "host": host,
        "port": int(port_raw) if port_raw else 3306,
        "user": user,
        "password": password,
        "database": database,
    }


def get_tabak_style_config() -> dict[str, Any] | None:
    """Mirrors the JDBC-URL-or-explicit-vars pattern from tabak_accuarcy.py."""
    jdbc_url = env("TABAK_DB_JDBC_URL")
    server, port, database = "", "3306", ""
    if jdbc_url:
        m = re.match(r"jdbc:(?:mariadb|mysql)://([^:/]+):?(\d+)?/(.+)", jdbc_url)
        if m:
            server, port, database = m.group(1), m.group(2) or "3306", m.group(3)
    if not server or not database:
        server = env("TABAK_DB_SERVER")
        port = env("TABAK_DB_PORT", default="3306")
        database = env("TABAK_DB_DATABASE")
    if not server or not database:
        return None
    return {
        "host": server,
        "port": int(port),
        "user": env("TABAK_DB_USERID"),
        "password": env("TABAK_DB_PASSWORD"),
        "database": database,
    }


def fetch_mysql_schema(cfg: dict[str, Any], label: str) -> list[dict[str, Any]]:
    import pymysql
    from pymysql.cursors import DictCursor

    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"],
        password=cfg["password"], database=cfg["database"],
        connect_timeout=15, charset="utf8mb4", cursorclass=DictCursor,
    )
    query = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            ORDINAL_POSITION,
            COLUMN_KEY AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    with conn.cursor() as cur:
        cur.execute(query, (cfg["database"],))
        rows = list(cur)
    conn.close()
    for r in rows:
        r["SOURCE"] = label
        r["IS_PRIMARY_KEY"] = "YES" if r.get("IS_PRIMARY_KEY") == "PRI" else "NO"
    return rows


# ----------------------------------------------------------------------
# Output
# ----------------------------------------------------------------------
def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {path}  ({len(rows)} columns)")


def write_json(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"  wrote {path}  ({len(rows)} columns)")


def print_summary(rows: list[dict[str, Any]], source: str) -> None:
    tables: dict[str, int] = {}
    for r in rows:
        tables.setdefault(r["TABLE_NAME"], 0)
        tables[r["TABLE_NAME"]] += 1
    print(f"\n[{source}] {len(tables)} tables, {len(rows)} columns total")
    for t, n in sorted(tables.items()):
        print(f"  - {t}: {n} columns")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument(
        "--only", choices=["all", "sqlserver", "mysql", "healthcare_ai", "tabak", "idp"], default="all"
    )
    args = parser.parse_args()

    all_rows: list[dict[str, Any]] = []

    if args.only in ("all", "idp"):
        cfg = get_idp_config()
        if cfg:
            print("Connecting to IDP SQL Server (dbo.vw_PdfClassificationTransactionLog) ...")
            try:
                rows = fetch_idp_schema(cfg)
                print_summary(rows, "idp")
                target_view_rows = [r for r in rows if r["TABLE_NAME"].lower() == "vw_pdfclassificationtransactionlog"]
                if target_view_rows:
                    print(f"  -> vw_PdfClassificationTransactionLog: {len(target_view_rows)} columns (the view idp_accuarcy.py actually queries)")
                all_rows.extend(rows)
                (write_csv if args.format == "csv" else write_json)(
                    rows, OUTPUT_DIR / f"idp_schema.{args.format}"
                )
            except Exception as ex:
                print(f"  IDP SQL Server connection/query failed: {ex}", file=sys.stderr)
        else:
            print("Skipping IDP DB (IDP_SQL_SERVER/IDP_SQL_DATABASE not set).")

    if args.only in ("all", "sqlserver"):
        cfg = get_sqlserver_config()
        if cfg:
            print("Connecting to SQL Server ...")
            try:
                rows = fetch_sqlserver_schema(cfg)
                print_summary(rows, "sqlserver")
                all_rows.extend(rows)
                (write_csv if args.format == "csv" else write_json)(
                    rows, OUTPUT_DIR / f"sqlserver_schema.{args.format}"
                )
            except Exception as ex:
                print(f"  SQL Server connection/query failed: {ex}", file=sys.stderr)
        else:
            print("Skipping SQL Server (SQLSERVER_HOST not set).")

    if args.only in ("all", "mysql"):
        cfg = get_mysql_config()
        if cfg:
            print("Connecting to MySQL ...")
            try:
                rows = fetch_mysql_schema(cfg, "mysql")
                print_summary(rows, "mysql")
                all_rows.extend(rows)
                (write_csv if args.format == "csv" else write_json)(
                    rows, OUTPUT_DIR / f"mysql_schema.{args.format}"
                )
            except Exception as ex:
                print(f"  MySQL connection/query failed: {ex}", file=sys.stderr)
        else:
            print("Skipping MySQL (MYSQL_HOST/MYSQL_DATABASE not set).")

    if args.only in ("all", "healthcare_ai"):
        cfg = get_healthcare_ai_config()
        if cfg:
            print("Connecting to Healthcare AI MySQL (EOB/Superbill fine-tuning source) ...")
            try:
                rows = fetch_mysql_schema(cfg, "healthcare_ai")
                print_summary(rows, "healthcare_ai")
                all_rows.extend(rows)
                (write_csv if args.format == "csv" else write_json)(
                    rows, OUTPUT_DIR / f"healthcare_ai_schema.{args.format}"
                )
            except Exception as ex:
                print(f"  Healthcare AI DB connection/query failed: {ex}", file=sys.stderr)
        else:
            print("Skipping Healthcare AI DB (HEALTHCARE_AI_DB_* not set).")

    if args.only in ("all", "tabak"):
        cfg = get_tabak_style_config()
        if cfg:
            print("Connecting to Tabak MariaDB ...")
            try:
                rows = fetch_mysql_schema(cfg, "tabak_mariadb")
                print_summary(rows, "tabak_mariadb")
                all_rows.extend(rows)
                (write_csv if args.format == "csv" else write_json)(
                    rows, OUTPUT_DIR / f"tabak_schema.{args.format}"
                )
            except Exception as ex:
                print(f"  Tabak DB connection/query failed: {ex}", file=sys.stderr)
        else:
            print("Skipping Tabak DB (TABAK_DB_* not set).")

    if all_rows:
        (write_csv if args.format == "csv" else write_json)(
            all_rows, OUTPUT_DIR / f"all_schemas_combined.{args.format}"
        )
    else:
        print("\nNo database connections were configured. Set the relevant "
              "env vars in .env and re-run.")


if __name__ == "__main__":
    main()