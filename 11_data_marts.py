import psycopg2

# These are the details to input for connection to PostGres, Please input your details : 

DB_NAME = "ClearSpend"
SCHEMA_NAME = "curated"
HOST = "localhost"
PORT = 2005
USER = "postgres"
PASSWORD = "Shahin@19"

# Connecting to Postgres

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)
conn.autocommit = True
cur = conn.cursor()


# We opted for Kimball-style dimensional data marts
# Conformed dimensions are shared across all marts and were already loaded by curated_layer.py
# Each department gets its own fact table with only the foreign keys and measures they need
# Teams join to the shared conformed dimensions as required for their specific questions
#
# Conformed dimensions available:
#   curated.dim_date
#   curated.dim_time
#   curated.dim_customers
#   curated.dim_cards
#   curated.dim_mcc
#   curated.dim_merchant_location
#   curated.dim_error


# Finance fact table : built for revenue reporting, refund analysis, and spend by geography and category
# fact_finance : dim_date, dim_merchant_location, dim_mcc, dim_error


print("Creating Finance fact table...")

cur.execute("DROP TABLE IF EXISTS curated.fact_finance CASCADE;")
cur.execute("""
    CREATE TABLE curated.fact_finance AS
    SELECT
        f.transaction_key,
        f.date_key,
        f.merchant_location_key,
        f.mcc_key,
        f.error_key,
        f.merchant_id,
        f.amount,
        f.is_refund,
        f.transaction_channel,
        f.use_chip
    FROM curated.fact_transactions f;
""")


print(f"fact_finance created")

# Customer analytics fact table : built for CLV, behavioural segmentation, and fraud patterns
# fact_customer : dim_date, dim_time, dim_customers, dim_cards, dim_error

print("\nCreating Customer Analytics fact table...")

cur.execute("DROP TABLE IF EXISTS curated.fact_customer CASCADE;")
cur.execute("""
    CREATE TABLE curated.fact_customer AS
    SELECT
        f.transaction_key,
        f.date_key,
        f.time_key,
        f.customer_key,
        f.card_key,
        f.error_key,
        f.amount,
        f.is_refund,
        f.transaction_channel,
        f.use_chip
    FROM curated.fact_transactions f;
""")

print(f"fact_customer created")


# Merchant partnerships fact table : built for volume reporting, industry growth, and error rate analysis
# fact_merchant : dim_date, dim_merchant_location, dim_mcc, dim_error


print("\nCreating Merchant Partnerships fact table...")

cur.execute("DROP TABLE IF EXISTS curated.fact_merchant CASCADE;")
cur.execute("""
    CREATE TABLE curated.fact_merchant AS
    SELECT
        f.transaction_key,
        f.date_key,
        f.merchant_location_key,
        f.mcc_key,
        f.error_key,
        f.merchant_id,
        f.amount,
        f.is_refund,
        f.transaction_channel,
        f.use_chip
    FROM curated.fact_transactions f;
""")

print(f"fact_merchant created")


cur.close()
conn.close()

print("\nAll dimensional data marts created successfully")