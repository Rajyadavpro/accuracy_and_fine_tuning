import json
from pathlib import Path

import pymysql


def main():
    cfg = json.loads(Path("local.settings.json").read_text(encoding="utf-8"))["Values"]
    conn = pymysql.connect(
        host=cfg["HEALTHCARE_AI_DB_SERVER"],
        port=int(cfg["HEALTHCARE_AI_DB_PORT"]),
        user=cfg["HEALTHCARE_AI_DB_USERID"],
        password=cfg["HEALTHCARE_AI_DB_PASSWORD"],
        database=cfg["HEALTHCARE_AI_DB_DATABASE"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT Id, rawJson FROM EOBAllocations WHERE Id = 226 LIMIT 1")
            r = cur.fetchone()
        data = json.loads(r[1])
        item = data["Allocation"]["Claims_Info"][0]["Claim"]["Service_Line_Items"][0]
        print("keys:")
        for k in item.keys():
            print("-", k)
        if "User_Status" in item:
            print("User_Status:", item["User_Status"])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
