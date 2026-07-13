import json
import re
from pathlib import Path

import pymysql

PAT = re.compile(r"user|adjust|correct|audit|final|edited|ground", re.IGNORECASE)


def walk(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            yield p, v
            yield from walk(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{prefix}[{i}]"
            yield p, v
            yield from walk(v, p)


def summarize(title, raw):
    data = json.loads(raw)
    matches = []
    for p, v in walk(data):
        if PAT.search(p):
            matches.append((p, type(v).__name__))
    print(f"=== {title} ===")
    print(f"keyword_paths={len(matches)}")
    for p, t in matches[:120]:
        print(f"{p} :: {t}")


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
            cur.execute("SELECT Id, rawJson FROM EOBAllocations WHERE rawJson IS NOT NULL AND rawJson != '' ORDER BY Id DESC LIMIT 1")
            eob = cur.fetchone()
            cur.execute("SELECT Id, RawJson FROM SuperBillAllocations WHERE RawJson IS NOT NULL AND RawJson != '' ORDER BY Id DESC LIMIT 1")
            sb = cur.fetchone()

        if eob:
            print(f"EOB id={eob[0]}")
            summarize("EOB", eob[1])
        if sb:
            print(f"SB id={sb[0]}")
            summarize("SUPERBILL", sb[1])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
