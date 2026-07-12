import pymysql
import json

conn = pymysql.connect(
    host='172.169.46.183',
    port=3306,
    user='root',
    password='CustomBPA#@!24#',
    database='BPACustomBPATabakProd',
    connect_timeout=30,
    charset='utf8mb4'
)
cursor = conn.cursor()

cursor.execute('SELECT VERSION()')
print('MySQL Version:', cursor.fetchone()[0])

cursor.execute('SELECT rawJson FROM EOBAllocations WHERE rawJson IS NOT NULL AND rawJson != "" LIMIT 1')
row = cursor.fetchone()
if row:
    data = json.loads(row[0])
    print('EOB JSON keys:', list(data.keys()))
    print('EOB JSON sample:', json.dumps(data, indent=2)[:500])

cursor.execute('SELECT RawJson FROM SuperBillAllocations WHERE RawJson IS NOT NULL AND RawJson != "" LIMIT 1')
row = cursor.fetchone()
if row:
    data = json.loads(row[0])
    print('\nSuperBill JSON keys:', list(data.keys()))
    print('SuperBill JSON sample:', json.dumps(data, indent=2)[:500])

conn.close()
