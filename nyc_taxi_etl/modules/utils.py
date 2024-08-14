
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, event

load_dotenv()

def postgres_connection():
    
    connection_url = os.getenv("CONNECTION_URL")

    db_conn = create_engine(connection_url)

    # Codigo para aumentar la velocidad de los inserts
    try:
        # Conectar al motor de base de datos
        connection = db_conn.connect()
        print("Conexión exitosa a PostgreSQL")
        return connection
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None
    
postgres_connection()