import psycopg2
from psycopg2.extras import RealDictCursor


# Database connection settings used by psycopg2.connect(**DB_CONFIG)
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "online_shop_sql",
    "user": "postgres",
    "password": "1254",
}


# Role:
#   Create and return a PostgreSQL connection using psycopg2.
# Inputs:
#   - None (uses DB_CONFIG defined in this module)
# Output:
#   - psycopg2.extensions.connection: an open DB connection (caller must close it)
# Notes:
#   - If the connection fails, the function raises the exception after logging.
def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print("❌ Error connecting to PostgreSQL:", e)
        raise
