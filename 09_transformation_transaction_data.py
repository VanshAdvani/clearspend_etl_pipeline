import psycopg2
import pandas as pd
import io
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# These are the details to input for connection to PostGres, Please input your details : 

DB_NAME = "ClearSpend"
SCHEMA_NAME = "transformation"
HOST = "localhost"
PORT = 2005
USER = "postgres"
PASSWORD = "Shahin@19"


# 1. EXTRACT

# Connecting to the database and pulling the raw transactions_data table

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM ingestion.transactions_data;")

rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]

df = pd.DataFrame(rows, columns=cols)


# 2. TRANSFORM

# US state codes used to distinguish domestic vs international merchant locations

US_STATES = {
    "AA","AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL",
    "GA","HI","IA","ID","IL","IN","KS","KY","LA","MA",
    "MD","ME","MI","MN","MO","MS","MT","NC","ND","NE",
    "NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI",
    "SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY"
}


# Strip leading/trailing whitespace from all string columns

for col in df.columns:
    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)


# Removing duplicate transaction IDs, keeping the first occurrence

df = df.drop_duplicates(subset=["id"], keep="first")

# Casting date to proper datetime : coerce anything unparseable to NaT

df["date"] = pd.to_datetime(df["date"], errors="coerce")

# Cleaning amount : strip $ and commas, cast to numeric
# Negatives are kept as-is since we assume they represent refunds/reimbursements

df["amount"] = df["amount"].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False)
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

# Lowercase use_chip for consistent values across the column

df["use_chip"] = df["use_chip"].astype(str).str.strip().str.lower()

# Deriving transaction_channel from merchant_city before we modify that column
# ONLINE city means it's an online transaction, everything else is in-store

df["merchant_city"] = df["merchant_city"].astype(str).str.strip()
df["transaction_channel"] = df["merchant_city"].str.upper().apply(
    lambda x: "online" if x == "ONLINE" else "in_store"
)

# Lowercase merchant_city and replace ONLINE with NA : that info is now in transaction_channel

df["merchant_city"] = df["merchant_city"].str.lower()
df.loc[df["merchant_city"] == "online", "merchant_city"] = "NA"
df.loc[df["merchant_city"] == "nan", "merchant_city"] = "NA"

# We created a new column merchant_country which has US, or country names if it is not US

df["merchant_state"] = df["merchant_state"].astype(str).str.strip()
df.loc[df["merchant_state"] == "nan", "merchant_state"] = "NA"

def split_state_country(val):
    if pd.isna(val) or val == "NA":
        return ("NA", "NA")
    val_upper = val.upper()
    if val_upper in US_STATES:
        return (val_upper, "US")
    else:
        return ("NA", val.title())

state_country = df["merchant_state"].apply(split_state_country)
df["merchant_country"] = state_country.apply(lambda x: x[1])
df["merchant_state"] = state_country.apply(lambda x: x[0])

# Online transactions have no physical location so both fields become NA

df.loc[df["transaction_channel"] == "online", "merchant_country"] = "NA"
df.loc[df["merchant_country"].isin(["None", "none", "nan", ""]), "merchant_country"] = "NA"

# Cleaning zip codes : remove float artifacts, pad short numeric zips to 5 digits
# Rows with a Bad Zipcode error are left unpadded and flagged separately to avoid anything misleading

df["zip"] = df["zip"].astype(str).str.strip()
df["zip"] = df["zip"].str.replace(r"\.0$", "", regex=True)
df.loc[df["zip"].isin(["nan", "None", "none", "null", ""]), "zip"] = "NA"

errors_lower = df["errors"].astype(str).str.strip().str.lower()

df["is_padded"] = "no"
df.loc[df["zip"] == "NA", "is_padded"] = "NA"

mask_bad_zip = (
    (df["zip"] != "NA") &
    (df["zip"].str.match(r"^\d+$")) &
    (errors_lower.str.contains("bad zipcode", na=False))
)
df.loc[mask_bad_zip, "is_padded"] = "no_because_error"

mask_to_pad = (
    (df["zip"] != "NA") &
    (df["zip"].str.match(r"^\d+$")) &
    (~errors_lower.str.contains("bad zipcode", na=False)) &
    (df["zip"].str.len() < 5)
)
df.loc[mask_to_pad, "zip"] = df.loc[mask_to_pad, "zip"].str.zfill(5)
df.loc[mask_to_pad, "is_padded"] = "yes"

errors_lower = df["errors"].astype(str).str.strip().str.lower()

# Keep mcc as a string : it's a foreign key that will join to mcc_data later

df["mcc"] = df["mcc"].astype(str).str.strip()

# Standardizing errors 

df["errors"] = df["errors"].astype(str).str.strip().str.lower()
df.loc[df["errors"] == "nan", "errors"] = "NA"
df.loc[df["errors"] == "", "errors"] = "NA"

# Filling any remaining nulls in string columns with NA, then doing a final full row dedup

string_cols = df.select_dtypes(include=["object"]).columns
df[string_cols] = df[string_cols].fillna("NA")
df = df.drop_duplicates()

# Replacing underscores with spaces and applying Title Case to all label like text columns
# Geographic and identifier columns (merchant_city, merchant_state, zip, mcc) are left as-is

# Replacing underscores with spaces and applying Title Case to all label like text columns
# Geographic and identifier columns (merchant_state, zip, mcc) are left as-is

def format_label(val):
    if pd.isna(val) or not isinstance(val, str):
        return val
    if val.strip().upper() == "NA":
        return "NA"
    return val.replace("_", " ").title()

for col in ["use_chip", "errors", "transaction_channel", "is_padded", "merchant_city"]:
    df[col] = df[col].apply(format_label)


# 3. LOAD

# Closing the extract connection, then reload a fresh one for the bulk COPY insert
# We use COPY instead of to_sql for performance : this table has 13M+ rows

cursor.close()
conn.close()

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)
cursor = conn.cursor()

# Drop the old table and recreate the schema using an empty to_sql call

cursor.execute("DROP TABLE IF EXISTS transformation.transactions_data;")
conn.commit()

url = URL.create(
    drivername="postgresql+psycopg2",
    username=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DB_NAME
)
engine = create_engine(url)
df.head(0).to_sql(
    name="transactions_data",
    con=engine,
    schema="transformation",  # To ensure no duplication
    if_exists="replace",
    index=False
)

# Streaming the full dataframe into Postgres via COPY for fast bulk loading

buffer = io.StringIO()
df.to_csv(buffer, index=False, header=True)
buffer.seek(0)

cursor.copy_expert("""
    COPY transformation.transactions_data
    FROM STDIN
    WITH (FORMAT csv, HEADER true)
""", buffer)

conn.commit()
cursor.close()
conn.close()

print("\nDataframe loaded into transformation.transactions_data")