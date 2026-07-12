import json
import os

def load_settings(filepath="local.settings.json"):
    if not os.path.exists(filepath):
        print(f"[-] Error: {filepath} not found.")
        return None
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get("Values", {})
    except json.JSONDecodeError as je:
        print(f"[-] JSON Syntax Error in {filepath}: {je}")
        print("    Please check for unclosed double quotes or missing commas.")
        return None
    except Exception as e:
        print(f"[-] Error reading {filepath}: {e}")
        return None

# 1. Langfuse Connection Check
def check_langfuse(values):
    pub_key = values.get("LANGFUSE_PUBLIC_KEY")
    sec_key = values.get("LANGFUSE_SECRET_KEY")
    host = values.get("LANGFUSE_HOST")

    if not all([pub_key, sec_key, host]):
        print("[-] Langfuse: Missing configuration keys.")
        return

    try:
        from langfuse import Langfuse
        lf = Langfuse(public_key=pub_key, secret_key=sec_key, host=host)
        # Auth check initiates a quick request to verify the credentials
        if lf.auth_check():
            print("[+] Langfuse: SUCCESS")
        else:
            print("[-] Langfuse: FAILED (Authentication check returned False)")
    except ImportError:
        print("[!] Langfuse: Skipped (Install 'langfuse' package to test)")
    except Exception as e:
        print(f"[-] Langfuse: FAILED. Error: {e}")

# 2. IDP SQL Server Check (Microsoft SQL Server)
def check_idp_sql(values):
    server = values.get("IDP_SQL_SERVER")
    database = values.get("IDP_SQL_DATABASE")
    user = values.get("IDP_SQL_USER")
    password = values.get("IDP_SQL_PASSWORD")

    if not all([server, database, user, password]):
        print("[-] IDP SQL: Missing credentials.")
        return

    # Attempt with pymssql first
    try:
        import pymssql
        host_part = server.split(",")[0]
        port_part = int(server.split(",")[1]) if "," in server else 1433
        
        conn = pymssql.connect(
            server=host_part,
            port=port_part,
            user=user,
            password=password,
            database=database,
            login_timeout=5
        )
        conn.close()
        print("[+] IDP SQL Database: SUCCESS (pymssql)")
        return
    except ImportError:
        pass
    except Exception as e:
        print(f"[-] IDP SQL Database (pymssql): FAILED. Error: {e}")
        return

    # Fallback attempt with pyodbc
    try:
        import pyodbc
        drivers = [d for d in pyodbc.drivers()]
        driver = drivers[0] if drivers else "ODBC Driver 17 for SQL Server"
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
        conn = pyodbc.connect(conn_str, timeout=5)
        conn.close()
        print(f"[+] IDP SQL Database: SUCCESS (pyodbc using {driver})")
    except ImportError:
        print("[!] IDP SQL Database: Skipped (Neither 'pymssql' nor 'pyodbc' is installed)")
    except Exception as e:
        print(f"[-] IDP SQL Database (pyodbc): FAILED. Error: {e}")

# 3. MySQL Database Check (Tabak & Healthcare AI)
def check_mysql_db(host, port, user, password, database, name):
    if not all([host, user, password, database]):
        print(f"[-] {name}: Missing configuration details.")
        return

    # Clean up JDBC URL formatting if stored in host key
    if host.startswith("jdbc:mysql://"):
        host = host.replace("jdbc:mysql://", "").split("/")[0].split(":")[0]

    try:
        import pymysql
        conn = pymysql.connect(
            host=host,
            port=int(port) if port else 3306,
            user=user,
            password=password,
            database=database,
            connect_timeout=5
        )
        conn.close()
        print(f"[+] {name}: SUCCESS")
    except ImportError:
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=host,
                port=int(port) if port else 3306,
                user=user,
                password=password,
                database=database,
                connection_timeout=5
            )
            conn.close()
            print(f"[+] {name}: SUCCESS (via mysql-connector)")
        except ImportError:
            print(f"[!] {name}: Skipped (Install 'pymysql' or 'mysql-connector-python')")
        except Exception as e:
            print(f"[-] {name}: FAILED. Error: {e}")
    except Exception as e:
         print(f"[-] {name}: FAILED. Error: {e}")

# 4. Azure Service Bus Check
def check_service_bus(values):
    connection_string = values.get("SERVICE_BUS_CONNECTION_STRING")
    queue_name = values.get("SERVICE_BUS_QUEUE_NAME")

    if not connection_string or "your-namespace" in connection_string:
        print("[-] Azure Service Bus: Skipped (using generic placeholder string)")
        return

    try:
        from azure.servicebus import ServiceBusClient
        with ServiceBusClient.from_connection_string(connection_string) as client:
            with client.get_queue_receiver(queue_name) as receiver:
                # peek_messages tests connection/permissions without pulling or modifying messages in the queue
                receiver.peek_messages(max_message_count=1)
                print("[+] Azure Service Bus: SUCCESS")
    except ImportError:
        print("[!] Azure Service Bus: Skipped (Install 'azure-servicebus')")
    except Exception as e:
        print(f"[-] Azure Service Bus: FAILED. Error: {e}")


def main():
    values = load_settings()
    if not values:
        return

    print("=== Checking Credentials ===\n")

    # 1. Check Langfuse
    check_langfuse(values)

    # 2. Check SQL Server (IDP)
    check_idp_sql(values)

    # 3. Check MySQL (Tabak)
    check_mysql_db(
        host=values.get("TABAK_DB_SERVER"),
        port=values.get("TABAK_DB_PORT"),
        user=values.get("TABAK_DB_USERID"),
        password=values.get("TABAK_DB_PASSWORD"),
        database=values.get("TABAK_DB_DATABASE"),
        name="MySQL (Tabak DB)"
    )

    # 4. Check MySQL (Healthcare AI)
    check_mysql_db(
        host=values.get("HEALTHCARE_AI_DB_SERVER"),
        port=values.get("HEALTHCARE_AI_DB_PORT"),
        user=values.get("HEALTHCARE_AI_DB_USERID"),
        password=values.get("HEALTHCARE_AI_DB_PASSWORD"),
        database=values.get("HEALTHCARE_AI_DB_DATABASE"),
        name="MySQL (Healthcare AI DB)"
    )

    # 5. Check Azure Service Bus
    check_service_bus(values)


if __name__ == "__main__":
    main()