import json
import sys
from pathlib import Path

import pymysql


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python AUX_code/peek_tabak_template_info_by_id.py <Transation_id>")
        return

    target_id = sys.argv[1]
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
                WHERE Transation_id = %s
                LIMIT 1
                """,
                (target_id,),
            )
            row = cur.fetchone()
            if not row:
                print("No row found")
                return

            print(f"id={row[0]}")
            txt = str(row[1])
            print(txt[:12000])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
