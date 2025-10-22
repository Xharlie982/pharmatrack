import os
import psycopg2
import pandas as pd
import boto3
from datetime import datetime

def run_ingestion():
    """
    Se conecta a PostgreSQL, extrae todas las tablas a archivos CSV,
    y los sube a un bucket de S3.
    """
    print("Iniciando el proceso de ingesta desde PostgreSQL...")
    try:
        db_host = os.environ["PG_HOST"]
        db_name = os.environ["PG_DB"]
        db_user = os.environ["PG_USER"]
        db_password = os.environ["PG_PASSWORD"]
        s3_bucket = os.environ["S3_BUCKET_NAME"]
    except KeyError as e:
        print(f"Error: La variable de entorno {e} no está definida.")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        print("Conexión a PostgreSQL exitosa.")
    except psycopg2.OperationalError as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return

    tablas = ["sucursal", "stock", "movimiento_stock"]
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    s3_client = boto3.client('s3')

    for tabla in tablas:
        try:
            print(f"Extrayendo datos de la tabla '{tabla}'...")
            query = f"SELECT * FROM {tabla};"
            df = pd.read_sql_query(query, conn)

            if df.empty:
                print(f"La tabla '{tabla}' está vacía. Saltando.")
                continue

            nombre_archivo = f"{tabla}.csv"
            # Crear directorio temporal si no existe
            os.makedirs("/tmp/ingesta_data", exist_ok=True)
            path_local = f"/tmp/ingesta_data/{nombre_archivo}"
            ruta_s3 = f"raw/inventario/{fecha_hoy}/{nombre_archivo}"

            df.to_csv(path_local, index=False)
            print(f"Archivo '{path_local}' creado con {len(df)} filas.")

            print(f"Subiendo '{path_local}' a S3 en la ruta '{ruta_s3}'...")
            s3_client.upload_file(path_local, s3_bucket, ruta_s3)
            print(f"Subida de '{tabla}' a S3 completada.")

            os.remove(path_local)

        except Exception as e:
            print(f"Error procesando la tabla '{tabla}': {e}")
        finally:
             # Limpiar archivo temporal si existe incluso si hubo error
            if 'path_local' in locals() and os.path.exists(path_local):
                 try:
                     os.remove(path_local)
                 except OSError:
                     pass # Ignorar error si no se puede borrar

    if conn:
        conn.close()
        print("Conexión a PostgreSQL cerrada.")

    print("Proceso de ingesta PostgreSQL finalizado.")

if __name__ == "__main__":
    run_ingestion()