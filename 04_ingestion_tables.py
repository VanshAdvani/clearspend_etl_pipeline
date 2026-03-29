import psycopg2
from psycopg2 import sql

# These are the details to input for connection to PostGres, Please input your details : 

DB_NAME = "ClearSpend"
SCHEMA_NAME = "ingestion"
HOST = "localhost"
PORT = 2005
USER = "postgres"
PASSWORD = "Shahin@19"


def main():

    # Connecting to the database

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DB_NAME,
        user=USER,
        password=PASSWORD,
    )

    try:
        with conn.cursor() as cur:

            # Creating the ingestion schema if it doesn't already exist

            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                .format(sql.Identifier(SCHEMA_NAME))
            )

            # Setting the search path so subsequent queries default to this schema

            cur.execute(
                sql.SQL("SET search_path TO {}")
                .format(sql.Identifier(SCHEMA_NAME))
            )

            # Users table : demographic and financial profile per user

            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.users_data (
                    id                  INT,
                    current_age         INT,
                    retirement_age      INT,
                    birth_year          VARCHAR(50),
                    birth_month         VARCHAR(50),
                    gender              VARCHAR(10),
                    address             VARCHAR(200),
                    latitude            NUMERIC(10, 6),
                    longitude           NUMERIC(10, 6),
                    per_capita_income   VARCHAR(50),
                    yearly_income       VARCHAR(50),
                    total_debt          VARCHAR(50),
                    credit_score        INT,
                    num_credit_cards    INT,
                    employment_status   VARCHAR(100),
                    education_level     VARCHAR(100)
                );
            """)

            # Cards table : one row per card, linked to a user via client_id

            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.cards_data (
                    id                      INT,
                    client_id               INT,
                    card_brand              VARCHAR(100),
                    card_type               VARCHAR(50),
                    card_number             VARCHAR(100),
                    expires                 VARCHAR(50),
                    cvv                     VARCHAR(20),
                    has_chip                VARCHAR(50),
                    num_cards_issued        VARCHAR(50),
                    credit_limit            VARCHAR(100),
                    acct_open_date          VARCHAR(50),
                    year_pin_last_changed   VARCHAR(50),
                    card_on_dark_web        VARCHAR(50),
                    issuer_bank_name        VARCHAR(50),
                    issuer_bank_state       VARCHAR(50),
                    issuer_bank_type        VARCHAR(50),
                    issuer_risk_rating      VARCHAR(50)
                );
            """)

            # MCC table : merchant category code reference data

            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.mcc_data (
                    code            VARCHAR(50),
                    description     VARCHAR(500),
                    notes           VARCHAR(200),
                    updated_by      VARCHAR(50)
                );
            """)

            # Transactions table : one row per transaction, linked to a card and user

            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.transactions_data (
                    id               INT,
                    date             VARCHAR(200),
                    client_id        INT,
                    card_id          INT,
                    amount           VARCHAR(200),
                    use_chip         VARCHAR(200),
                    merchant_id      INT,
                    merchant_city    VARCHAR(200),
                    merchant_state   VARCHAR(200),
                    zip              VARCHAR(200),
                    mcc              VARCHAR(200),
                    errors           VARCHAR(200)
                );
            """)

        conn.commit()
        print(f"All 4 tables created successfully in schema '{SCHEMA_NAME}' inside database '{DB_NAME}'")

    except Exception as e:

        # Rolling back if anything fails so we don't end up with partial table creation

        conn.rollback()
        print("Error while creating tables : ")
        print(e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()