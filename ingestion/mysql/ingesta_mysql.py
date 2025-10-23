import os
import pymysql 
import pandas as pd
import boto3
from datetime import datetime
from urllib.parse import urlparse
from sqlalchemy import create_engine 

def run_ingestion():
    """
    Se conecta a MySQL, extrae todas las tablas a archivos CSV,
    y los sube a un bucket de S3 usando SQLAlchemy para mayor compatibilidad con Pandas.
    """
    print("Iniciando el proceso de ingesta desde MySQL...")
    try:

        db_url_env = os.environ["MYSQL_URL"] 
        s3_bucket = os.environ["S3_BUCKET_NAME"]

        parsed_url_pymysql = urlparse(db_url_env.replace("mysql+pymysql", "mysql"))
        db_host = parsed_url_pymysql.hostname
        db_port = parsed_url_pymysql.port or 3306
        db_name = parsed_url_pymysql.path.lstrip('/')
        db_user = parsed_url_pymysql.username
        db_password = parsed_url_pymysql.password
        if not all([db_host, db_name, db_user, db_password]):
            raise ValueError("MYSQL_URL inválida o incompleta.")

        db_url_sqlalchemy = db_url_env 

    except KeyError as e:
        print(f"Error: La variable de entorno {e} no está definida.")
        return
    except Exception as e:
        print(f"Error parseando MYSQL_URL o configurando: {e}")
        return

    conn_pymysql = None 
    engine_sqlalchemy = None 
    
    try:

        conn_pymysql = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Conexión (PyMySQL) a MySQL exitosa para listar tablas.")

        tablas = []
        with conn_pymysql.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            result = cursor.fetchall()
            tablas = [list(row.values())[0] for row in result]
            print(f"Tablas encontradas: {tablas}")
        
        conn_pymysql.close()
        print("Conexión (PyMySQL) cerrada.")

        engine_sqlalchemy = create_engine(db_url_sqlalchemy)
        print("Engine de SQLAlchemy creado exitosamente.")

        fecha_hoy = datetime.now().strftime('%Y-%m-%d')
        s3_client = boto3.client('s3')

        for tabla in tablas:
            df = pd.DataFrame() 
            path_local = None 
            try:
                print(f"Extrayendo datos de la tabla '{tabla}'...")
                query = f"SELECT * FROM `{tabla}`;"

                df = pd.read_sql(query, engine_sqlalchemy) 

                if df.empty:
                    print(f"La tabla '{tabla}' está vacía o pd.read_sql falló silenciosamente. Saltando.")
                    continue
                
                if len(df) > 0 and tuple(df.columns) == tuple(df.iloc[0]):
                    print(f"¡ADVERTENCIA! Parece que solo se leyó el encabezado para la tabla '{tabla}'. Saltando esta tabla.")
                    continue

                print(f"Se leyeron {len(df)} filas de la tabla '{tabla}'.")

                for col in df.select_dtypes(include=['datetime64[ns]', 'datetime', 'datetimetz']).columns:
                    try:

                        if pd.api.types.is_datetime64_any_dtype(df[col].dtype) and df[col].dt.tz is not None:
                            df[col] = pd.to_datetime(df[col]).dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        else:
                             df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    except Exception as dt_err:
                        print(f"Advertencia: No se pudo convertir la columna datetime '{col}' en tabla '{tabla}'. Error: {dt_err}. Se intentará formato simple.")

                        try:
                           df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                        except Exception as dt_err_alt:
                            print(f"Fallo formato simple también para '{col}'. Se dejará como está. Error: {dt_err_alt}")


                nombre_archivo = f"{tabla}.csv"
                os.makedirs("/tmp/ingesta_data", exist_ok=True)
                path_local = f"/tmp/ingesta_data/{nombre_archivo}"
                
                df.to_csv(path_local, index=False, date_format='%Y-%m-%dT%H:%M:%SZ') 
                print(f"Archivo '{path_local}' creado con {len(df)} filas.")

                ruta_s3 = f"raw/recetas/{tabla}/{fecha_hoy}/{nombre_archivo}"
                print(f"Subiendo '{path_local}' a S3 en la ruta '{ruta_s3}'...")
                s3_client.upload_file(path_local, s3_bucket, ruta_s3)
                print(f"Subida de '{tabla}' a S3 completada.")

            except Exception as e:
                print(f"Error GRANDE procesando la tabla '{tabla}': {e}")
            finally:

                if path_local and os.path.exists(path_local):
                    try:
                        os.remove(path_local)
                        print(f"Archivo temporal '{path_local}' eliminado.")
                    except OSError as oe:
                         print(f"Error eliminando archivo temporal '{path_local}': {oe}")

    except pymysql.MySQLError as e:
        print(f"Error de conexión PyMySQL inicial: {e}")
    except Exception as e:
        print(f"Error general durante el proceso: {e}")
    finally:

        if conn_pymysql and conn_pymysql.open:
            conn_pymysql.close()
            print("Conexión PyMySQL cerrada (en finally).")
        if engine_sqlalchemy:
            engine_sqlalchemy.dispose() 
            print("Engine de SQLAlchemy dispuesto (conexiones cerradas).")

    print("Proceso de ingesta MySQL finalizado.")

if __name__ == "__main__":
    run_ingestion()