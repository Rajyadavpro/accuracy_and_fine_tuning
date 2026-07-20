#!/usr/bin/env python
import pymysql

# Try connecting to different databases to find which has Allocations table
databases = ['BPACustomBPATabakProd', 'WFM-D', 'BPACustomBPA_EOB', 'healthcare_ai', 'eob_healthcare', 'BPACustomBPA']

for db in databases:
    try:
        conn = pymysql.connect(
            host='172.169.46.183',
            port=3306,
            user='root',
            password='CustomBPA#@!24#',
            database=db,
            connect_timeout=5
        )
        cursor = conn.cursor()
        cursor.execute('SHOW TABLES LIKE "Allocations"')
        result = cursor.fetchall()
        status = '✓ FOUND Allocations table' if result else '✗ No Allocations table'
        print(f'{db}: {status}')
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'{db}: ERROR - {str(e)[:60]}')
