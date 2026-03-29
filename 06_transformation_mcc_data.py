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

# Connecting to the database and pulling the raw mcc_data table

conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM ingestion.mcc_data;")

rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]

df = pd.DataFrame(rows, columns=cols)


# 2. TRANSFORM


# Strip leading/trailing whitespace from all string columns

for col in df.columns:
    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)


# Cleaning the code column : removed quotes, MCC prefix variants, and extra whitespace

df["code"] = df["code"].astype(str).str.strip()
df["code"] = df["code"].str.replace('"', '', regex=False)
df["code"] = df["code"].str.replace("MCC", "", regex=False)
df["code"] = df["code"].str.replace("mcc", "", regex=False)
df["code"] = df["code"].str.strip()

# Keeping only rows where code is a valid numeric string

df = df[df["code"].str.fullmatch(r"\d+")]


# Standardizing description to lowercase for consistent mapping later

df["description"] = df["description"].astype(str).str.strip().str.lower()


# Filling empty notes with NA

df["notes"] = df["notes"].apply(lambda x: x.strip() if isinstance(x, str) else x)
df["notes"] = df["notes"].replace("", pd.NA)
df["notes"] = df["notes"].fillna("NA")


# Standardizing updated_by : fill blanks, lowercase, and anonymize the name "john"

df["updated_by"] = df["updated_by"].apply(lambda x: x.strip() if isinstance(x, str) else x)
df["updated_by"] = df["updated_by"].replace("", pd.NA)
df["updated_by"] = df["updated_by"].fillna("unknown")
df["updated_by"] = df["updated_by"].astype(str).str.strip().str.lower()

df["updated_by"] = df["updated_by"].replace({
    "john": "personal_user"
})


# Mapping each description to a higher level industry category

industry_map = {
    # Food & Dining
    "fast food restaurants": "food and dining",
    "eating places and restaurants": "food and dining",
    "miscellaneous food stores": "food and dining",
    "drinking places (alcoholic beverages)": "food and dining",
    "package stores, beer, wine, liquor": "food and dining",

    # Healthcare
    "hospitals": "healthcare",
    "doctors, physicians": "healthcare",
    "dentists and orthodontists": "healthcare",
    "optometrists, optical goods and eyeglasses": "healthcare",
    "chiropractors": "healthcare",
    "podiatrists": "healthcare",
    "medical services": "healthcare",
    "drug stores and pharmacies": "healthcare",

    # Transportation
    "airlines": "transportation",
    "railroad passenger transport": "transportation",
    "bus lines": "transportation",
    "taxicabs and limousines": "transportation",
    "cruise lines": "transportation",
    "passenger railways": "transportation",
    "local and suburban commuter transportation": "transportation",
    "railroad freight": "transportation",
    "motor freight carriers and trucking": "transportation",
    "tolls and bridge fees": "transportation",
    "towing services": "transportation",

    # Retail
    "department stores": "retail",
    "grocery stores, supermarkets": "retail",
    "discount stores": "retail",
    "wholesale clubs": "retail",
    "family clothing stores": "retail",
    "women's ready-to-wear stores": "retail",
    "shoe stores": "retail",
    "sports apparel, riding apparel stores": "retail",
    "book stores": "retail",
    "gift, card, novelty stores": "retail",
    "antique shops": "retail",
    "cosmetic stores": "retail",
    "leather goods": "retail",

    # Technology
    "computers computer peripheral equipment": "technology",
    "electronics stores": "technology",
    "computer network services": "technology",
    "telecommunication services": "technology",
    "semiconductors and related devices": "technology",
    "digital goods - media, books, apps": "technology",
    "digital goods - games": "technology",
    "cable, satellite, and other pay television services": "technology",

    # Home & Garden
    "hardware stores": "home and garden",
    "lumber and building materials": "home and garden",
    "furniture, home furnishings, and equipment stores": "home and garden",
    "miscellaneous home furnishing stores": "home and garden",
    "household appliance stores": "home and garden",
    "lawn and garden supply stores": "home and garden",
    "gardening supplies": "home and garden",
    "floor covering stores": "home and garden",
    "upholstery and drapery stores": "home and garden",
    "lighting fixtures electrical supplies": "home and garden",
    "florists supplies, nursery stock and flowers": "home and garden",

    # Automotive
    "service stations": "automotive",
    "automotive parts and accessories stores": "automotive",
    "automotive body repair shops": "automotive",
    "automotive service shops": "automotive",
    "car washes": "automotive",

    # Entertainment & Recreation
    "motion picture theaters": "entertainment",
    "amusement parks, carnivals, circuses": "entertainment",
    "theatrical producers": "entertainment",
    "betting (including lottery tickets, casinos)": "entertainment",
    "athletic fields, commercial sports": "entertainment",
    "recreational sports, clubs": "entertainment",
    "sporting goods stores": "entertainment",
    "music stores - musical instruments": "entertainment",
    "artist supply stores, craft shops": "entertainment",

    # Manufacturing
    "tools, parts, supplies manufacturing": "manufacturing",
    "miscellaneous fabricated metal products": "manufacturing",
    "coated and laminated products": "manufacturing",
    "non-ferrous metal foundries": "manufacturing",
    "steelworks": "manufacturing",
    "steel products manufacturing": "manufacturing",
    "steel drums and barrels": "manufacturing",
    "miscellaneous metals": "manufacturing",
    "miscellaneous metalwork": "manufacturing",
    "miscellaneous metal fabrication": "manufacturing",
    "fabricated structural metal products": "manufacturing",
    "bolt, nut, screw, rivet manufacturing": "manufacturing",
    "ironwork": "manufacturing",
    "non-precious metal services": "manufacturing",
    "heat treating metal services": "manufacturing",
    "electroplating, plating, polishing services": "manufacturing",
    "welding repair": "manufacturing",
    "pottery and ceramics": "manufacturing",
    "brick stone and related materials": "manufacturing",
    "miscellaneous machinery and parts manufacturing": "manufacturing",
    "industrial equipment and supplies": "manufacturing",
    "ship chandlers": "manufacturing",

    # Professional Services
    "legal services and attorneys": "professional services",
    "accounting, auditing, and bookkeeping services": "professional services",
    "tax preparation services": "professional services",
    "insurance sales, underwriting": "professional services",
    "detective agencies, security services": "professional services",

    # Travel & Lodging
    "lodging - hotels, motels, resorts": "travel and lodging",
    "travel agencies": "travel and lodging",

    # Personal Services
    "beauty and barber shops": "personal services",
    "laundry services": "personal services",
    "cleaning and maintenance services": "personal services",
    "heating, plumbing, air conditioning contractors": "personal services",

    # Financial Services
    "money transfer": "financial services",

    # Government & Utilities
    "utilities - electric, gas, water, sanitary": "utilities",
    "postal services - government only": "utilities",

    # Books & Media
    "books, periodicals, newspapers": "books and media",

    # Precious Goods
    "precious stones and metals": "precious goods",
}

df["industry"] = df["description"].map(industry_map).fillna("other")


# Dropping full duplicates first, then deduplicated on code keeping the first occurrence

df = df.drop_duplicates()
df = df.drop_duplicates(subset=["code"], keep="first")


# Replacing underscores with spaces and applying Title Case to all label like text columns

def format_label(val):
    if pd.isna(val) or not isinstance(val, str):
        return val
    if val.strip().upper() == "NA":
        return "NA"
    return val.replace("_", " ").title()

df["description"] = df["description"].apply(format_label)
df["notes"]       = df["notes"].apply(format_label)
df["updated_by"]  = df["updated_by"].apply(format_label)
df["industry"]    = df["industry"].apply(format_label)

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
    name="mcc_data",
    con=engine,
    schema="transformation",
    if_exists="replace",  # To ensure no duplication
    index=False
)

print("\nDataframe loaded into transformation.mcc_data")