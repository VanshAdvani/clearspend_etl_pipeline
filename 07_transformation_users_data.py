import psycopg2
import pandas as pd
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

# Connecting to the database and pulling the raw users_data table

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM ingestion.users_data;")

rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]

df = pd.DataFrame(rows, columns=cols)

# 2. TRANSFORM

# Strip leading/trailing whitespace from all string columns

for col in df.columns:
    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

# Below we are handling the duplicates (Also considering the SCD types 1 and types 2

## Step 1 : removing true duplicates (identical across every column)


df = df.drop_duplicates()


## Step 2 : group by id and all Type 2 columns so each distinct combination is preserved
### Example - If two rows have the same id and same Type 2 values but different address : same group, last address stays (Type 1)
### Example - If two rows have the same id but different credit_score : different groups, both stay (Type 2)

scd2_group_cols = ["id", "yearly_income", "total_debt", "credit_score", "employment_status", "education_level"]

df = (
    df.groupby(scd2_group_cols, sort=False)
      .last()
      .reset_index()
)


# Checking birth_year + current_age consistency : the dataset appears to be from ~2019-2020
# so we flag rows where the implied data year falls outside the expected 2018-2021 window

df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce")
df["current_age"] = pd.to_numeric(df["current_age"], errors="coerce")
df["implied_data_year"] = df["birth_year"] + df["current_age"]
inconsistent = df[~df["implied_data_year"].between(2018, 2021)]
df = df.drop(columns=["implied_data_year"])

# Cleaning monetary columns : strip $, commas, handle 'k' shorthand, and cast to int

def clean_money(val):
    if pd.isna(val):
        return None
    val = str(val).strip()
    val = val.replace("$", "").replace(",", "")
    if val.lower().endswith("k"):
        val = val[:-1]
        try:
            return int(float(val) * 1000)
        except ValueError:
            return None
    try:
        return int(float(val))
    except ValueError:
        return None

df["per_capita_income"] = df["per_capita_income"].apply(clean_money)
df["yearly_income"] = df["yearly_income"].apply(clean_money)

# Cleaning total_debt the same way : strip $ and commas then cast to integer

df["total_debt"] = df["total_debt"].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False)
df["total_debt"] = pd.to_numeric(df["total_debt"], errors="coerce").astype("Int64")

# Log any zero or null per_capita_income values for awareness : we keep them as-is

zero_pci = (df["per_capita_income"] == 0).sum()

invalid_rows = df[df["per_capita_income"].isna()]

# Standardizing employment_status : fixing typos and collapse variants into clean labels

df["employment_status"] = df["employment_status"].astype(str).str.strip().str.lower()
df["employment_status"] = df["employment_status"].str.replace(r"[^a-z\- ]", "", regex=True)

employment_map = {
    "employed":       "employed",
    "employd":        "employed",
    "emplyed":        "employed",
    "empled":         "employed",
    "self-employed":  "self-employed",
    "self employed":  "self-employed",
    "self-employd":   "self-employed",
    "student":        "student",
    "studnt":         "student",
    "retired":        "retired",
    "retird":         "retired",
    "ret":            "retired",
    "ret.":           "retired",
    "unemployed":     "unemployed",
    "unemployd":      "unemployed",
    "un-employed":    "unemployed",
}
df["employment_status"] = df["employment_status"].map(employment_map).fillna(df["employment_status"])

# Standardizing education_level : fix abbreviations and typos into consistent labels

df["education_level"] = df["education_level"].astype(str).str.strip().str.lower()
df["education_level"] = df["education_level"].str.replace(r"\s+", " ", regex=True)

education_map = {
    "bachelor degree":    "bachelor",
    "bachelor's degree":  "bachelor",
    "bachelors":          "bachelor",
    "ba/bs":              "bachelor",
    "high school":        "high_school",
    "highschool":         "high_school",
    "hs":                 "high_school",
    "associate degree":   "associate",
    "associate deg.":     "associate",
    "assoc degree":       "associate",
    "associate":          "associate",
    "master degree":      "master",
    "master's degree":    "master",
    "masters":            "master",
    "ms/ma":              "master",
    "doctorate":          "doctorate",
}
df["education_level"] = df["education_level"].map(education_map).fillna(df["education_level"])

# Gender just needs whitespace stripped : values are already clean

df["gender"] = df["gender"].astype(str).str.strip()


# Final full row dedup and dropping the current_age column (redundant given birth_year)
# And also to avoid SCD issues of overwriting the current age every year

df = df.drop_duplicates()
df = df.drop(columns=["current_age"])


# Replacing underscores with spaces and applying Title Case to all label like text columns

def format_label(val):
    if pd.isna(val) or not isinstance(val, str):
        return val
    if val.strip().upper() == "NA":
        return "NA"
    return val.replace("_", " ").title()

df["employment_status"] = df["employment_status"].apply(format_label)
df["education_level"]   = df["education_level"].apply(format_label)
df["gender"]            = df["gender"].apply(format_label)


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
    name="users_data",
    con=engine,
    schema="transformation",
    if_exists="replace",  # To ensure no duplication
    index=False
)

print("\nDataframe loaded into transformation.users_data")