import os
import pymysql
import pandas as pd
import boto3
from datetime import datetime
from urllib.parse import urlparse

def run_ingestion():
    """
    Se conecta a MySQL, extrae todas las tablas a archivos CSV,
    y los sube a un bucket de S3.
    """
    print("Iniciando el proceso de ingesta desde MySQL...")
    try:
        db_url = os.environ["MYSQL_URL"]
        s3_bucket = os.environ["S3_BUCKET_NAME"]

        parsed_url = urlparse(db_url.replace("mysql+pymysql", "mysql"))
        db_host = parsed_url.hostname
        db_port = parsed_url.port or 3306
        db_name = parsed_url.path.lstrip('/')
        db_user = parsed_url.username
        db_password = parsed_url.password
        if not all([db_host, db_name, db_user, db_password]):
             raise ValueError("MYSQL_URL inválida o incompleta.")

    except KeyError as e:
        print(f"Error: La variable de entorno {e} no está definida.")
        return
    except Exception as e:
        print(f"Error parseando MYSQL_URL: {e}")
        return

    conn = None
    try:
        conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Conexión a MySQL exitosa.")
    except pymysql.MySQLError as e:
        print(f"Error al conectar a MySQL: {e}")
        return

    tablas = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            result = cursor.fetchall()
            tablas = [list(row.values())[0] for row in result]
            print(f"Tablas encontradas: {tablas}")
    except pymysql.MySQLError as e:
        print(f"Error obteniendo lista de tablas: {e}")
        if conn: conn.close()
        return

    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    s3_client = boto3.client('s3')

    for tabla in tablas:
        try:
            print(f"Extrayendo datos de la tabla '{tabla}'...")
            query = f"SELECT * FROM `{tabla}`;"
            df = pd.read_sql(query, conn)

            if df.empty:
                print(f"La tabla '{tabla}' está vacía. Saltando.")
                continue

            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                 # Intenta convertir asegurando que es timezone-naive y luego a ISO UTC
                 try:
                     df[col] = pd.to_datetime(df[col]).dt.tz_localize(None).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                 except Exception as dt_err:
                     print(f"Advertencia: No se pudo convertir la columna datetime '{col}' en tabla '{tabla}'. Error: {dt_err}. Se dejará como está.")


            nombre_archivo = f"{tabla}.csv"
            os.makedirs("/tmp/ingesta_data", exist_ok=True)
            path_local = f"/tmp/ingesta_data/{nombre_archivo}"
            ruta_s3 = f"raw/recetas/{tabla}/{fecha_hoy}/{nombre_archivo}"

            df.to_csv(path_local, index=False)
            print(f"Archivo '{path_local}' creado con {len(df)} filas.")

            print(f"Subiendo '{path_local}' a S3 en la ruta '{ruta_s3}'...")
            s3_client.upload_file(path_local, s3_bucket, ruta_s3)
            print(f"Subida de '{tabla}' a S3 completada.")

            os.remove(path_local)

        except Exception as e:
            print(f"Error procesando la tabla '{tabla}': {e}")
        finally:
            if 'path_local' in locals() and os.path.exists(path_local):
                 try:
                     os.remove(path_local)
                 except OSError:
                     pass

    if conn:
        conn.close()
        print("Conexión a MySQL cerrada.")

    print("Proceso de ingesta MySQL finalizado.")

if __name__ == "__main__":
    run_ingestion()