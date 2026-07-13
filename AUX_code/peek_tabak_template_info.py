import json
from pathlib import Path

import pymysql


def main() -> None:
    cfg = json.loads(Path("local.settings.json").read_text(encoding="utf-8"))["Values"]

    conn = pymysql.connect(
        host=cfg["TABAK_DB_SERVER"],
        port=int(cfg.get("TABAK_DB_PORT", "3306")),
        user=cfg["TABAK_DB_USERID"],
        password=cfg["TABAK_DB_PASSWORD"],
        database=cfg["TABAK_DB_DATABASE"],
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT Transation_id, template_info
                FROM Transactions
                WHERE template_info IS NOT NULL
                  AND template_info != ''
                ORDER BY Transation_id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                print("No rows found")
                return

            print(f"id={row[0]}")
            txt = str(row[1])
            print(txt[:7000])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
