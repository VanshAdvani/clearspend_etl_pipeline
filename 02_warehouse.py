import psycopg2

# These are the details to input for connection to PostGres, Please input your details : 

NEW_DB_NAME = "ClearSpend"   # The name of the Data Warehouse is ClearSpend
HOST = "localhost"
PORT = 2005
USER = "postgres"
PASSWORD = "Shahin@19"

# New database creation

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname="postgres",
    user=USER,
    password=PASSWORD,
)

conn.autocommit = True

try:
    with conn.cursor() as cur:

        # check if DB already exists

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (NEW_DB_NAME,))
        exists = cur.fetchone() is not None

        if exists:
            print(f"Database '{NEW_DB_NAME}' already exists.")
        else:
            cur.execute(f'CREATE DATABASE "{NEW_DB_NAME}";')
            print(f"Database '{NEW_DB_NAME}' created successfully ")

finally:
    conn.close()

