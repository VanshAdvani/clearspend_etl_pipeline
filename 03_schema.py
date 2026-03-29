import psycopg2

# These are the details to input for connection to PostGres, Please input your details : 

conn = psycopg2.connect(
    host="localhost",
    port=2005,
    dbname="ClearSpend",
    user="postgres",
    password="Shahin@19"
)

conn.autocommit = True

# Creating the three layers of our pipeline

schemas = ["ingestion", "transformation", "curated"]

with conn.cursor() as cur:
    for schema in schemas:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        print(f"Schema '{schema}' created successfully")

conn.close()