import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import io

# These are the details to input for connection to PostGres, Please input your details : 

DB_NAME = "ClearSpend"
SCHEMA_NAME = "curated"
HOST = "localhost"
PORT = 2005
USER = "postgres"
PASSWORD = "Shahin@19"

# Connecting to Postgres and extracting cleaned data from the transformation layer

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)
cursor = conn.cursor()


url = URL.create(
    drivername="postgresql+psycopg2",
    username=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DB_NAME
)
engine = create_engine(url)

# Loading cleaned tables into pandas DataFrames for building dimensions and facts

with engine.connect() as conn_sa:
    users_df        = pd.read_sql("SELECT * FROM transformation.users_data;", con=conn_sa)
    cards_df        = pd.read_sql("SELECT * FROM transformation.cards_data;", con=conn_sa)
    mcc_df          = pd.read_sql("SELECT * FROM transformation.mcc_data;", con=conn_sa)
    transactions_df = pd.read_sql("SELECT * FROM transformation.transactions_data;", con=conn_sa)


# Building a date dimension covering a fixed range to support time based analysis

print("\nBuilding dim_date...")

date_range = pd.date_range(start="2010-01-01", end="2025-12-31", freq="D")
dim_date = pd.DataFrame({
    "date_key":     date_range.strftime("%Y%m%d").astype(int),
    "full_date":    date_range,
    "year":         date_range.year,
    "quarter":      date_range.quarter,
    "month":        date_range.month,
    "month_name":   date_range.strftime("%B"),
    "day":          date_range.day,
    "day_of_week":  date_range.dayofweek,
    "day_name":     date_range.strftime("%A"),
    "is_weekend":   date_range.dayofweek.isin([5, 6])
})

# Creating a time dimension for detailed transaction timings

print("\nBuilding dim_time...")

time_range = pd.date_range(start="00:00:00", end="23:59:59", freq="s")
dim_time = pd.DataFrame({
    "time_key":  time_range.strftime("%H%M%S").astype(int),
    "full_time": time_range.strftime("%H:%M:%S"),
    "hour":      time_range.hour,
    "minute":    time_range.minute,
    "second":    time_range.second
})

# Building customer dimension with SCD Type 2 support
# Type 2 tracked attributes: yearly_income, total_debt, credit_score, employment_status, education_level
# Each distinct combination of these values gets its own row with its own surrogate key
# All other attributes are Type 1 (overwritten) and were already resolved in transformation
# customer_id is the natural key, customer_key is the surrogate key 

print("\nBuilding dim_customers...")

dim_customers = pd.DataFrame({
    "customer_id":       users_df["id"],
    "retirement_age":    users_df["retirement_age"],
    "birth_year":        users_df["birth_year"],
    "birth_month":       users_df["birth_month"],
    "gender":            users_df["gender"],
    "address":           users_df["address"],
    "latitude":          users_df["latitude"],
    "longitude":         users_df["longitude"],
    "per_capita_income": users_df["per_capita_income"],
    "yearly_income":     users_df["yearly_income"],
    "total_debt":        users_df["total_debt"],
    "credit_score":      users_df["credit_score"],
    "num_credit_cards":  users_df["num_credit_cards"],
    "employment_status": users_df["employment_status"],
    "education_level":   users_df["education_level"]
})

# Sort by customer_id so versions of the same customer are grouped together
# Within each customer, row order represents chronological version order (earliest first)

dim_customers = dim_customers.sort_values(["customer_id"]).reset_index(drop=True)
dim_customers.insert(0, "customer_key", dim_customers.index + 1) # Surrogate key (unique per version)

# SCD Type 2 metadata : the last version per customer is marked as current
# Earlier versions are kept for historical reference but marked is_current = False
# The fact table joins on is_current = True to link transactions to the active version
# In production, effective_start_date and effective_end_date columns could be added (But it was out of our scope)
# to support date-range joins for historically accurate reporting

dim_customers["is_current"] = dim_customers.groupby("customer_id").cumcount() + 1 == \
                               dim_customers.groupby("customer_id")["customer_id"].transform("count")


# Building card dimension with SCD Type 2 support
# credit_limit is the Type 2 tracked attribute : each distinct value gets its own row
# All other attributes are Type 1 (overwritten) and were already resolved in transformation
# card_id is the natural key, card_key is the surrogate key 

print("\nBuilding dim_cards...")

dim_cards = pd.DataFrame({
    "card_id":               cards_df["id"],
    "customer_id":           cards_df["client_id"],
    "card_brand":            cards_df["card_brand"],
    "card_type":             cards_df["card_type"],
    "card_number":           cards_df["card_number"],
    "expires":               cards_df["expires"],
    "cvv":                   cards_df["cvv"],
    "cvv_quality":           cards_df["cvv_quality"],
    "has_chip":              cards_df["has_chip"],
    "num_cards_issued":      cards_df["num_cards_issued"],
    "credit_limit":          cards_df["credit_limit"],
    "credit_limit_flag":     cards_df["credit_limit_flag"],
    "acct_open_date":        cards_df["acct_open_date"],
    "year_pin_last_changed": cards_df["year_pin_last_changed"],
    "card_on_dark_web":      cards_df["card_on_dark_web"],
    "issuer_bank_name":      cards_df["issuer_bank_name"],
    "issuer_bank_state":     cards_df["issuer_bank_state"],
    "issuer_bank_type":      cards_df["issuer_bank_type"],
    "issuer_risk_rating":    cards_df["issuer_risk_rating"]
})

dim_cards = dim_cards.sort_values(["card_id"]).reset_index(drop=True)
dim_cards.insert(0, "card_key", dim_cards.index + 1) # Surrogate key (unique per version)

# SCD Type 2 metadata : the last version per card is marked as current
# Earlier versions are kept for historical reference but marked is_current = False
# The fact table joins on is_current = True to link transactions to the active version
# In production, effective_start_date and effective_end_date columns could be added (But it was out of our scope)
# to support date-range joins for historically accurate reporting

dim_cards["is_current"] = dim_cards.groupby("card_id").cumcount() + 1 == \
                           dim_cards.groupby("card_id")["card_id"].transform("count")

# Masking sensitive values while keeping last 4 digits for traceability

dim_cards["cvv"] = dim_cards["cvv"].apply(
    lambda x: "***" if x != "NA" else "NA"
)
dim_cards["card_number"] = dim_cards["card_number"].astype(str).apply(
    lambda x: "*" * (len(x) - 4) + x[-4:] if len(x) >= 4 and x != "NA" else "NA"
)

# Building MCC dimension to classify merchants by industry and category

print("\nBuilding dim_mcc...")

dim_mcc = pd.DataFrame({
    "mcc_code":        mcc_df["code"],
    "mcc_description": mcc_df["description"],
    "industry":        mcc_df["industry"],
    "notes":           mcc_df["notes"],
    "updated_by":      mcc_df["updated_by"]
})

dim_mcc = dim_mcc.sort_values("mcc_code").reset_index(drop=True)
dim_mcc.insert(0, "mcc_key", dim_mcc.index + 1) # Surrogate key

# Creating a merchant location dimension so each merchant location combination is uniquely tracked

print("\nBuilding dim_merchant_location...")

merchant_locations = transactions_df.groupby(
    ["merchant_id", "merchant_city", "merchant_state", "merchant_country", "zip", "is_padded"]
).size().reset_index(name="transaction_count")

dim_merchant_location = merchant_locations.sort_values(
    ["merchant_id", "transaction_count"], ascending=[True, False]
).reset_index(drop=True)

dim_merchant_location.insert(0, "merchant_location_key", dim_merchant_location.index + 1) # Surrogate key


# Building error dimension by extracting unique error combinations and creating flags for each type

print("\nBuilding dim_error...")

errors_series = transactions_df["errors"].astype(str).str.strip().str.lower()
errors_series = errors_series.replace({"nan": "NA", "": "NA"})

unique_errors = errors_series.unique()

dim_error = pd.DataFrame({"error_string": unique_errors})
dim_error["has_bad_cvv"]              = dim_error["error_string"].str.contains("bad cvv", case=False, na=False)
dim_error["has_bad_card_number"]      = dim_error["error_string"].str.contains("bad card number", case=False, na=False)
dim_error["has_bad_zipcode"]          = dim_error["error_string"].str.contains("bad zipcode", case=False, na=False)
dim_error["has_insufficient_balance"] = dim_error["error_string"].str.contains("insufficient balance", case=False, na=False)
dim_error["has_technical_glitch"]     = dim_error["error_string"].str.contains("technical glitch", case=False, na=False)
dim_error["has_bad_pin"]              = dim_error["error_string"].str.contains("bad pin", case=False, na=False)
dim_error["has_bad_expiration"]       = dim_error["error_string"].str.contains("bad expiration", case=False, na=False)
dim_error["has_error"]                = ~dim_error["error_string"].isin(["NA", "na"])
dim_error["error_count"] = dim_error[["has_bad_cvv", "has_bad_card_number", "has_bad_zipcode",
                                       "has_insufficient_balance", "has_technical_glitch",
                                       "has_bad_pin", "has_bad_expiration"]].sum(axis=1)

dim_error = dim_error.sort_values("error_string").reset_index(drop=True)
dim_error.insert(0, "error_key", dim_error.index + 1) # Surrogate key


# Building fact table by joining transactions with all dimensions and surrogate keys

print("\nBuilding fact_transactions...")

df = transactions_df

# Extracting date and time keys for linking to time dimensions

df["date"] = pd.to_datetime(df["date"])
df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
df["time_key"] = df["date"].dt.strftime("%H%M%S").astype(int)

# Linking transactions to customer dimension using only the current version (SCD Type 2)
# In production with multiple versions per customer, this join would also filter on
# effective_start_date and effective_end_date to match the transaction date to the
# correct historical version of the customer

dim_customers_current = dim_customers[dim_customers["is_current"] == True][["customer_key", "customer_id"]]
df = pd.merge(df, dim_customers_current,
              how="left", left_on="client_id", right_on="customer_id")

# Linking transactions to card dimension using only the current version (SCD Type 2)
# In production with multiple versions per card, this join would also filter on
# effective_start_date and effective_end_date to match the transaction date to the
# correct historical version of the card

dim_cards_current = dim_cards[dim_cards["is_current"] == True][["card_key", "card_id"]]
df = pd.merge(df, dim_cards_current,
              how="left", on="card_id")

# Linking to MCC dimension using standardized codes

df["mcc"] = df["mcc"].astype(str)
dim_mcc["mcc_code"] = dim_mcc["mcc_code"].astype(str)
df = pd.merge(df, dim_mcc[["mcc_key", "mcc_code"]],
              how="left", left_on="mcc", right_on="mcc_code")

# Linking to merchant-location dimension for precise geographic tracking

df = pd.merge(
    df,
    dim_merchant_location[["merchant_location_key", "merchant_id", "merchant_city", "merchant_state", "merchant_country", "zip", "is_padded"]],
    how="left",
    on=["merchant_id", "merchant_city", "merchant_state", "merchant_country", "zip", "is_padded"]
)

# Linking to error dimension after cleaning error strings

df["errors_clean"] = df["errors"].astype(str).str.strip().str.lower()
df.loc[df["errors_clean"].isin(["nan", ""]), "errors_clean"] = "NA"
df = pd.merge(df, dim_error[["error_key", "error_string"]],
              how="left", left_on="errors_clean", right_on="error_string")

# Final fact table containing all foreign keys and measurable attributes

fact_transactions = pd.DataFrame({
    "date_key":             df["date_key"],
    "time_key":             df["time_key"],
    "customer_key":         df["customer_key"],
    "card_key":             df["card_key"],
    "merchant_location_key":df["merchant_location_key"],
    "mcc_key":              df["mcc_key"],
    "error_key":            df["error_key"],
    "merchant_id":          df["merchant_id"],
    "amount":               df["amount"],
    "use_chip":             df["use_chip"],
    "transaction_channel":  df["transaction_channel"],
    "is_refund":            df["amount"] < 0,
})

fact_transactions.insert(0, "transaction_key", fact_transactions.index + 1) # Surrogate key


# Loading all dimension and fact tables into the curated schema

print("\nLoading into curated schema...")

cursor.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()

# Dropping existing objects to avoid dependency conflicts during reload

cursor.execute("""
    DO $$ 
    DECLARE r RECORD;
    BEGIN
        FOR r IN (SELECT viewname FROM pg_views WHERE schemaname = 'curated') LOOP
            EXECUTE 'DROP VIEW IF EXISTS curated.' || quote_ident(r.viewname) || ' CASCADE';
        END LOOP;
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'curated') LOOP
            EXECUTE 'DROP TABLE IF EXISTS curated.' || quote_ident(r.tablename) || ' CASCADE';
        END LOOP;
    END $$;
""")
conn.commit()

# Load dimensions using SQLAlchemy

dim_date.to_sql(name="dim_date", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_date loaded")

dim_time.to_sql(name="dim_time", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_time loaded")

dim_customers.to_sql(name="dim_customers", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_customers loaded")

dim_cards.to_sql(name="dim_cards", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_cards loaded")

dim_mcc.to_sql(name="dim_mcc", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_mcc loaded")

dim_merchant_location.to_sql(name="dim_merchant_location", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_merchant_location loaded")

dim_error.to_sql(name="dim_error", con=engine, schema="curated", if_exists="replace", index=False)
print("dim_error loaded")

# Creating empty fact table structure, then bulk load using COPY for performance

fact_transactions.head(0).to_sql(
    name="fact_transactions", con=engine, schema="curated", if_exists="replace", index=False
)

buffer = io.StringIO()
fact_transactions.to_csv(buffer, index=False, header=True)
buffer.seek(0)

cursor.copy_expert("""
    COPY curated.fact_transactions
    FROM STDIN
    WITH (FORMAT csv, HEADER true)
""", buffer)

conn.commit()
print("fact_transactions loaded")

cursor.close()
conn.close()

print("\nCurated layer complete, star schema ready")
print("Run curated_data_marts.py next to create the data mart views.")