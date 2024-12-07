import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="DB_HOST",
            database="DB_NAME",
            user="DB_USER",
            password="DB_PASSWORD",
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        raise
