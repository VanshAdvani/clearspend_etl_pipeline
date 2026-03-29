import psycopg2 # Main Package to connect python to SQL Server (PostGres)

# Initialising database connection of Python to PostGres
# These are the details to input for connection to PostGres, Please input your details : 

conn = psycopg2.connect(
    host="localhost",
    port=2005,
    dbname="postgres",
    user="postgres",      
    password="Shahin@19"
)

with conn.cursor() as cur:
    cur.execute("SELECT current_database(), current_user, version();")
    print(cur.fetchone())

conn.close()