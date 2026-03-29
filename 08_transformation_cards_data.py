import psycopg2
import pandas as pd
import re
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

# Connect and pull both cards and transactions — we need transactions to assess CVV quality

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM ingestion.cards_data;")
rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=cols)

cursor.execute("SELECT * FROM ingestion.transactions_data;")
txn_rows = cursor.fetchall()
txn_cols = [desc[0] for desc in cursor.description]
df_txn = pd.DataFrame(txn_rows, columns=txn_cols)


# 2. TRANSFORM


# Strip leading/trailing whitespace from all string columns

for col in df.columns:
    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

# Below we are handling the duplicates (Also considering the SCD types 1 and types 2

## Step 1 : removing true duplicates (identical across every column)
length_before_dedup = len(df)
df = df.drop_duplicates()

## Step 2 : group by id and all Type 2 columns so each distinct combination is preserved
### Example :  If two rows have the same id and same Type 2 values but different address : same group, last address stays (Type 1)
### Example : If two rows have the same id but different credit_limit : different groups, both stay (Type 2)

df = (
    df.groupby(["id", "client_id", "credit_limit"], sort=False)
      .last()
      .reset_index()
)

# Standardize card_brand — collapse typos, symbols, and aliases into the four known brands
# Anything unrecognizable or missing becomes NA

df["card_brand"] = df["card_brand"].astype(str).str.strip().str.lower()
df["card_brand"] = df["card_brand"].str.replace(r"\s+", "", regex=True)
df["card_brand"] = df["card_brand"].str.replace("!", "", regex=False)
df["card_brand"] = df["card_brand"].str.replace("-", "", regex=False)

brand_map = {
    "visa":        "visa",
    "v":           "visa",
    "vvisa":       "visa",
    "vissa":       "visa",
    "visacard":    "visa",
    "vis":         "visa",
    "vsa":         "visa",
    "mastercard":  "mastercard",
    "amex":        "amex",
    "discover":    "discover",
    "nan":         "NA",
    "none":        "NA",
    "unknown":     "NA",
    "":            "NA",
}
df["card_brand"] = df["card_brand"].map(brand_map).fillna(df["card_brand"])
df["card_brand"] = df["card_brand"].fillna("NA")

# Catch any remaining variants that slipped through the map
df.loc[df["card_brand"].str.contains("visa", na=False), "card_brand"] = "visa"
df.loc[df["card_brand"].str.contains("master", na=False), "card_brand"] = "mastercard"
df.loc[df["card_brand"].str.contains("amex", na=False) | df["card_brand"].str.contains("ame", na=False), "card_brand"] = "amex"
df.loc[df["card_brand"].str.contains("discov", na=False) | df["card_brand"].str.contains("dis", na=False), "card_brand"] = "discover"

# Final safety net to make sure no "none" strings survived the catch-all contains checks
df.loc[df["card_brand"].str.lower() == "none", "card_brand"] = "NA"


# Standardize card_type — prepaid check comes first since those values also contain "debit"

df["card_type"] = df["card_type"].astype(str).str.strip().str.lower()
df["card_type"] = df["card_type"].str.replace(r"\s+", " ", regex=True)

def map_card_type(val):
    if pd.isna(val) or val in ("nan", "unknown", ""):
        return "NA"
    if any(kw in val for kw in ["prepaid", "prepiad", "pre payed", "ppd", "dpp", "db-pp", "dp"]):
        return "debit_prepaid"
    if any(kw in val for kw in ["credit", "crdit", "credt", "crdeit", "cedit", "cr", "cc", "cred"]):
        return "credit"
    if any(kw in val for kw in ["debit", "debiit", "debti", "deibt", "de bit", "deb", "db", "d"]):
        return "debit"
    return "NA"

df["card_type"] = df["card_type"].apply(map_card_type)


# Clean card_number — remove float artifacts like .0 or .5 introduced during ingestion

df["card_number"] = (
    df["card_number"]
    .astype(str)
    .str.strip()
    .str.replace(r"\.0$", "", regex=True)
    .str.replace(r"\.5$", "", regex=True)
)


# CVV cleaning — pad short CVVs to 3 digits, but skip cards where ALL transactions
# had Bad CVV errors, since padding those would be misleading

txn_errors = df_txn[["card_id", "errors"]].copy()
txn_errors["has_bad_cvv"] = txn_errors["errors"].astype(str).str.contains("Bad CVV", case=False, na=False)
cvv_by_card = txn_errors.groupby("card_id").agg(
    total_txns=("has_bad_cvv", "count"),
    bad_cvv_txns=("has_bad_cvv", "sum")
).reset_index()

only_bad_cvv_card_ids = set(
    cvv_by_card.loc[
        cvv_by_card["total_txns"] == cvv_by_card["bad_cvv_txns"],
        "card_id"
    ].astype(int)
)

df["cvv"] = df["cvv"].astype(str).str.strip()
df.loc[df["cvv"].isin(["", "nan", "None", "none", "null", "0"]), "cvv"] = "NA"

only_bad_cvv_mask = df["id"].isin(only_bad_cvv_card_ids)
short_cvv_mask = df["cvv"].str.fullmatch(r"\d{1,2}", na=False)
df.loc[short_cvv_mask & ~only_bad_cvv_mask, "cvv"] = df.loc[short_cvv_mask & ~only_bad_cvv_mask, "cvv"].str.zfill(3)
df.loc[df["cvv"] == "000", "cvv"] = "NA"

# Tag each CVV with a quality label so downstream users know what they're working with
df["cvv_quality"] = "valid"
df.loc[short_cvv_mask & ~only_bad_cvv_mask, "cvv_quality"] = "padded"
df.loc[only_bad_cvv_mask & short_cvv_mask, "cvv_quality"] = "bad_cvv"
df.loc[only_bad_cvv_mask & ~short_cvv_mask & (df["cvv"] != "NA"), "cvv_quality"] = "bad_cvv"
df.loc[df["cvv"] == "NA", "cvv_quality"] = "missing"


# has_chip just needs whitespace stripped — values are already clean

df["has_chip"] = df["has_chip"].astype(str).str.strip()


# Clean credit_limit — handle $, commas, 'k' notation, text values, and cast to int
# We keep a raw copy first so we can accurately track what was changed and why

df["credit_limit_raw"] = df["credit_limit"]

def clean_credit_limit(val):
    if pd.isna(val):
        return pd.NA
    val = str(val).strip().lower()
    if val in ("error_value", "limit_unknown", "unknown", ""):
        return pd.NA
    if val == "ten thousand":
        result = 10000
    else:
        val = val.replace("$", "").replace(",", "")
        if val.endswith("k"):
            try:
                result = float(val[:-1]) * 1000
            except ValueError:
                return pd.NA
        else:
            try:
                result = float(val)
            except ValueError:
                return pd.NA
    return int(result)

df["credit_limit"] = df["credit_limit"].apply(clean_credit_limit)

invalid_input_mask = df["credit_limit"].isna() & df["credit_limit_raw"].notna()

negative_mask = df["credit_limit"].notna() & (df["credit_limit"] < 0)

original_zero_mask = (
    df["credit_limit_raw"].notna()
    & df["credit_limit_raw"].apply(lambda x: str(x).strip().replace("$", "").replace(",", "") == "0"
        or str(x).strip().replace("$", "").replace(",", "") == "0.0")
)

# Set negatives and unparseable strings to 0 — they'll be flagged below
df.loc[negative_mask, "credit_limit"] = 0
df.loc[invalid_input_mask, "credit_limit"] = 0
df["credit_limit"] = pd.to_numeric(df["credit_limit"], errors="coerce").fillna(0).astype("Int64")

# Build masks for anomaly detection — thresholds differ between credit and debit cards
credit_mask = df["card_type"] == "credit"
debit_mask = df["card_type"].isin(["debit", "debit_prepaid"])

credit_too_high_mask = credit_mask & (df["credit_limit"] > 500000)
credit_too_small_mask = credit_mask & (df["credit_limit"] > 0) & (df["credit_limit"] < 100)
debit_too_high_mask = debit_mask & (df["credit_limit"] > 500000)
nines_mask = (
    df["credit_limit"].astype(str).str.fullmatch(r"9{4,}")
    & (df["credit_limit"] > 0)
)

# Flag every anomaly type so analysts can filter or investigate later
df["credit_limit_flag"] = "normal"
df.loc[negative_mask, "credit_limit_flag"] = "negative_input"
df.loc[invalid_input_mask, "credit_limit_flag"] = "invalid_input"
df.loc[original_zero_mask, "credit_limit_flag"] = "original_zero"
df.loc[nines_mask, "credit_limit_flag"] = "placeholder_nines"
df.loc[credit_too_small_mask, "credit_limit_flag"] = "too_small_credit"
df.loc[credit_too_high_mask, "credit_limit_flag"] = "too_high_credit"
df.loc[debit_too_high_mask, "credit_limit_flag"] = "too_high_debit"

remaining_zero_mask = (df["credit_limit"] == 0) & (df["credit_limit_flag"] == "normal")
df.loc[remaining_zero_mask, "credit_limit_flag"] = "zero_from_cleaning"

# Drop the raw column now that flagging is done
df = df.drop(columns=["credit_limit_raw"])


# Standardize acct_open_date to Mon-YY format across all input variants
# Dates that can't be parsed or are in the future get set to NA

def clean_acct_open_date(val):
    if pd.isna(val):
        return "NA"
    val = str(val).strip().lower()
    if val in ("not available", "unknown", "nan", ""):
        return "NA"
    if re.match(r'^[a-z]{3}-\d{2}$', val):
        return val.title()
    from dateutil import parser
    try:
        parsed = parser.parse(val, dayfirst=False)
        if parsed.year > 2025:
            return "NA"
        return parsed.strftime("%b-%y")
    except (ValueError, OverflowError):
        return "NA"

df["acct_open_date"] = df["acct_open_date"].apply(clean_acct_open_date)


# Date consistency checks — pin year is treated as the more reliable source
# If pin year predates the open date, we correct the open date to Jan of the pin year

valid_date_mask = (
    df["acct_open_date"].str.match(r'^[A-Za-z]{3}-\d{2}$', na=False) &
    df["expires"].str.match(r'^[A-Za-z]{3}-\d{2}$', na=False)
)
cards_valid_dates = df[valid_date_mask].copy()
cards_valid_dates["open_parsed"] = pd.to_datetime("01-" + cards_valid_dates["acct_open_date"], format="%d-%b-%y", errors="coerce")
cards_valid_dates["expires_parsed"] = pd.to_datetime("01-" + cards_valid_dates["expires"], format="%d-%b-%y", errors="coerce")

expires_before_open = cards_valid_dates[cards_valid_dates["expires_parsed"] < cards_valid_dates["open_parsed"]]

valid_pin_mask = (
    df["acct_open_date"].str.match(r'^[A-Za-z]{3}-\d{2}$', na=False) &
    df["year_pin_last_changed"].astype(str).str.match(r'^\d{4}$', na=False)
)
cards_valid_pin = df[valid_pin_mask].copy()
cards_valid_pin["open_parsed"] = pd.to_datetime("01-" + cards_valid_pin["acct_open_date"], format="%d-%b-%y", errors="coerce")
cards_valid_pin["pin_year"] = cards_valid_pin["year_pin_last_changed"].astype(int)
cards_valid_pin["open_year"] = cards_valid_pin["open_parsed"].dt.year

pin_before_open = cards_valid_pin[cards_valid_pin["pin_year"] < cards_valid_pin["open_year"]]

bad_pin_mask = cards_valid_pin["pin_year"] < cards_valid_pin["open_year"]
for _, row in cards_valid_pin.loc[bad_pin_mask].iterrows():
    pin_yr = row["pin_year"]
    new_date = f"Jan-{str(pin_yr)[-2:]}"
    df.loc[df["id"] == row["id"], "acct_open_date"] = new_date

# Impute any remaining NA open dates using the pin year as a best estimate
na_open_mask = (
    (df["acct_open_date"] == "NA") &
    (df["year_pin_last_changed"].astype(str).str.match(r'^\d{4}$', na=False))
)
na_open_count = na_open_mask.sum()
for idx in df.loc[na_open_mask].index:
    pin_yr = str(df.loc[idx, "year_pin_last_changed"]).strip()
    new_date = f"Jan-{pin_yr[-2:]}"
    df.loc[idx, "acct_open_date"] = new_date

# Re-run the pin consistency check to confirm corrections worked
valid_pin_mask = (
    df["acct_open_date"].str.match(r'^[A-Za-z]{3}-\d{2}$', na=False) &
    df["year_pin_last_changed"].astype(str).str.match(r'^\d{4}$', na=False)
)
cards_valid_pin = df[valid_pin_mask].copy()
cards_valid_pin["open_parsed"] = pd.to_datetime("01-" + cards_valid_pin["acct_open_date"], format="%d-%b-%y", errors="coerce")
cards_valid_pin["pin_year"] = cards_valid_pin["year_pin_last_changed"].astype(int)
cards_valid_pin["open_year"] = cards_valid_pin["open_parsed"].dt.year

pin_before_open = cards_valid_pin[cards_valid_pin["pin_year"] < cards_valid_pin["open_year"]]

future_pin = cards_valid_pin[cards_valid_pin["pin_year"] > 2025]

ids_to_check = [4938, 967, 2399, 4090, 3650]
df.loc[df["id"].isin(ids_to_check), ["id", "acct_open_date", "year_pin_last_changed"]]


# Lowercase card_on_dark_web — values are yes/no so just need consistent casing

df["card_on_dark_web"] = df["card_on_dark_web"].astype(str).str.strip().str.lower()


# Standardize issuer_bank_name — collapse abbreviations and spacing variants into clean keys

df["issuer_bank_name"] = df["issuer_bank_name"].astype(str).str.strip().str.lower()
df["issuer_bank_name"] = df["issuer_bank_name"].str.replace(r"\s+", " ", regex=True)

bank_name_map = {
    "citi":             "citi",
    "ally bank":        "ally_bank",
    "ally bk":          "ally_bank",
    "chase bank":       "chase_bank",
    "chase bk":         "chase_bank",
    "truist":           "truist",
    "wells fargo":      "wells_fargo",
    "capital one":      "capital_one",
    "discover bank":    "discover_bank",
    "discover bk":      "discover_bank",
    "bank of america":  "bank_of_america",
    "bk of america":    "bank_of_america",
    "pnc bank":         "pnc_bank",
    "pnc bk":           "pnc_bank",
    "jpmorgan chase":   "jpmorgan_chase",
    "jp morgan chase":  "jpmorgan_chase",
    "u.s. bank":        "us_bank",
    "u.s. bk":          "us_bank",
}
df["issuer_bank_name"] = df["issuer_bank_name"].map(bank_name_map).fillna(df["issuer_bank_name"])


# Map full state names and lowercase abbreviations to standard 2-letter uppercase codes

df["issuer_bank_state"] = df["issuer_bank_state"].astype(str).str.strip().str.lower()

state_map = {
    "new york":       "NY",
    "ny":             "NY",
    "north carolina": "NC",
    "nc":             "NC",
    "michigan":       "MI",
    "mi":             "MI",
    "pennsylvania":   "PA",
    "pa":             "PA",
    "virginia":       "VA",
    "va":             "VA",
    "california":     "CA",
    "ca":             "CA",
    "illinois":       "IL",
    "il":             "IL",
    "minnesota":      "MN",
    "mn":             "MN",
}
df["issuer_bank_state"] = df["issuer_bank_state"].map(state_map).fillna(df["issuer_bank_state"])


# Collapse bank type variants into three clean labels: national, online, regional

df["issuer_bank_type"] = df["issuer_bank_type"].astype(str).str.strip().str.lower()
df["issuer_bank_type"] = df["issuer_bank_type"].str.replace(r"\s+", " ", regex=True)

bank_type_map = {
    "national":       "national",
    "national bank":  "national",
    "online":         "online",
    "online only":    "online",
    "online bank":    "online",
    "regional":       "regional",
    "regional bank":  "regional",
}
df["issuer_bank_type"] = df["issuer_bank_type"].map(bank_type_map).fillna(df["issuer_bank_type"])


# Standardize risk rating — only two valid values expected: low and medium

df["issuer_risk_rating"] = df["issuer_risk_rating"].astype(str).str.strip().str.lower()

risk_map = {
    "low":       "low",
    "low risk":  "low",
    "medium":    "medium",
    "med":       "medium",
}
df["issuer_risk_rating"] = df["issuer_risk_rating"].map(risk_map).fillna(df["issuer_risk_rating"])


# Fill any remaining nulls in string columns with NA, then do a final full-row dedup

string_cols = df.select_dtypes(include=["object"]).columns
df[string_cols] = df[string_cols].fillna("NA")
df = df.drop_duplicates()


# Replace underscores with spaces and apply Title Case to all label-like text columns
# issuer_bank_state is intentionally excluded since it uses 2-letter uppercase codes

def format_label(val):
    if pd.isna(val) or not isinstance(val, str):
        return val
    if val.strip().upper() == "NA":
        return "NA"
    return val.replace("_", " ").title()

for col in ["card_brand", "card_type", "has_chip", "card_on_dark_web",
            "issuer_bank_name", "issuer_bank_type", "issuer_risk_rating",
            "cvv_quality", "credit_limit_flag"]:
    df[col] = df[col].apply(format_label)


# 3. LOAD

# Closing the extract connection and loading the cleaned data into the transformation schema

cursor.close()
conn.close()

url = URL.create(
    drivername="postgresql+psycopg2",
    username=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DB_NAME
)

engine = create_engine(url)

df.to_sql(
    name="cards_data",
    con=engine,
    schema="transformation",
    if_exists="replace",  # To ensure no duplication
    index=False
)

print("\nDataframe loaded into transformation.cards_data")