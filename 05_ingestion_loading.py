import psycopg2
from psycopg2 import sql

# These are the details to input for connection to PostGres, Please input your details : 

DB_NAME = "ClearSpend"
SCHEMA_NAME = "ingestion"
HOST = "localhost"
PORT = 2005
USER = "postgres"
PASSWORD = "Shahin@19"

# Please input your file path for the datasets : 

USERS_FILE        = "/Users/vansh/Desktop/Dataset-final-project/users_data.csv"
CARDS_FILE        = "/Users/vansh/Desktop/Dataset-final-project/cards_data.csv"
MCC_FILE          = "/Users/vansh/Desktop/Dataset-final-project/mcc_data.csv"
TRANSACTIONS_FILE = "/Users/vansh/Desktop/Dataset-final-project/transactions_data.csv"


# Truncate table before loading to avoid duplicate data on reruns

def truncate_table(cur, table_name):
    cur.execute(
        sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE")
        .format(sql.Identifier(SCHEMA_NAME, table_name))
    )



def load_small_file(cur, table_name, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            sql=f"""
                COPY {SCHEMA_NAME}.{table_name}
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """,
            file=f
        )

# Loading large files in chunks

def load_large_file_chunked(cur, table_name, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            sql=f"""
                COPY {SCHEMA_NAME}.{table_name}
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """,
            file=f
        )


def main():

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DB_NAME,
        user=USER,
        password=PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(SCHEMA_NAME)))

            # Truncate before loading

            truncate_table(cur, "users_data")
            load_small_file(cur, "users_data", USERS_FILE)
            print("users_data loaded")

            truncate_table(cur, "cards_data")
            load_small_file(cur, "cards_data", CARDS_FILE)
            print("cards_data loaded")

            truncate_table(cur, "mcc_data")
            load_small_file(cur, "mcc_data", MCC_FILE)
            print("mcc_data loaded")

            truncate_table(cur, "transactions_data")
            load_large_file_chunked(cur, "transactions_data", TRANSACTIONS_FILE)
            print("transactions_data loaded")

        conn.commit()
        print("All tables loaded successfully!")

    except Exception as e:
        conn.rollback()
        print("Error occurred : ")
        print(e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()