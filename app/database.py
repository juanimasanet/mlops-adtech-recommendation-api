import psycopg2
from psycopg2.extras import RealDictCursor

# Variables de conexión
db_username = 'postgres'
db_password = 'GRUPO3RDS2024'
db_host = 'grupo-3-25-11.cf4i6e6cwv74.us-east-1.rds.amazonaws.com'
db_port = '5432'
db_name = 'grupo_3'

def get_db_connection():
    """
    Establece y devuelve una conexión a la base de datos PostgreSQL.
    """
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_username,
            password=db_password,
            port=db_port,
            cursor_factory=RealDictCursor  
        )
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        raise
