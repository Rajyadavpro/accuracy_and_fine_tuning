import json
from pathlib import Path

import pymysql


def walk(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v)


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
            cur.execute("SELECT Id, rawJson FROM EOBAllocations WHERE rawJson IS NOT NULL AND rawJson != '' ORDER BY Id DESC LIMIT 200")
            rows = cur.fetchall()

        found = 0
        for rid, raw in rows:
            try:
                data = json.loads(raw)
            except Exception:
                continue

            non_empty_user_values = []
            for k, v in walk(data):
                if isinstance(k, str) and k.startswith("User_"):
                    val = v.get("value") if isinstance(v, dict) and "value" in v else v
                    sval = "" if val is None else str(val).strip()
                    if sval:
                        non_empty_user_values.append((k, sval[:80]))

            if non_empty_user_values:
                found += 1
                print(f"Id={rid} non_empty_user_fields={len(non_empty_user_values)}")
                for k, v in non_empty_user_values[:8]:
                    print(f"  {k} = {v}")
                if found >= 5:
                    break

        if found == 0:
            print("No non-empty User_* values found in latest 200 EOB rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
