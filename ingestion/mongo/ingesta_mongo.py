import os
import pymongo
import pandas as pd 
import boto3
from datetime import datetime
from bson import json_util
import json
from urllib.parse import urlparse

def run_ingestion():
    """
    Se conecta a MongoDB, extrae todas las colecciones a archivos JSON Lines,
    y los sube a un bucket de S3.
    """
    print("Iniciando el proceso de ingesta desde MongoDB...")
    try:
        db_url = os.environ["MONGO_URL"]
        s3_bucket = os.environ["S3_BUCKET_NAME"]
        
        parsed_url = urlparse(db_url)
        db_name = parsed_url.path.lstrip('/')
        if not db_name:
             raise ValueError("No se pudo extraer el nombre de la base de datos de MONGO_URL")

    except KeyError as e:
        print(f"Error: La variable de entorno {e} no está definida.")
        return
    except Exception as e:
        print(f"Error configurando MongoDB: {e}")
        return

    client = None
    db = None
    try:
        client = pymongo.MongoClient(db_url)
        client.admin.command('ping')
        db = client[db_name]
        print(f"Conexión a MongoDB (base de datos '{db_name}') exitosa.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error al conectar a MongoDB (falla de conexión): {e}")
        return
    except pymongo.errors.OperationFailure as e:
         print(f"Error al conectar a MongoDB (falla de operación, ej. auth): {e}")
         return
    except Exception as e:
        print(f"Error inesperado al conectar a MongoDB: {e}")
        return

    colecciones = [col for col in db.list_collection_names() if not col.startswith('system.')]
    print(f"Colecciones encontradas: {colecciones}")

    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    s3_client = boto3.client('s3')

    for coleccion_nombre in colecciones:
        cursor = None 
        path_local = None 
        try:
            print(f"Extrayendo datos de la colección '{coleccion_nombre}'...")
            collection = db[coleccion_nombre]
            cursor = collection.find({})
            
            documentos = list(cursor)

            if not documentos:
                print(f"La colección '{coleccion_nombre}' está vacía. Saltando.")
                continue

            nombre_archivo = f"{coleccion_nombre}.jsonl"
            os.makedirs("/tmp/ingesta_data", exist_ok=True)
            path_local = f"/tmp/ingesta_data/{nombre_archivo}"
            ruta_s3 = f"raw/catalogo/{coleccion_nombre}/{fecha_hoy}/{nombre_archivo}"

            with open(path_local, 'w') as f:
                for doc in documentos:

                    f.write(json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS) + '\n')

            print(f"Archivo '{path_local}' creado con {len(documentos)} documentos.")

            print(f"Subiendo '{path_local}' a S3 en la ruta '{ruta_s3}'...")
            s3_client.upload_file(path_local, s3_bucket, ruta_s3)
            print(f"Subida de '{coleccion_nombre}' a S3 completada.")

            os.remove(path_local)

        except Exception as e:
            print(f"Error procesando la colección '{coleccion_nombre}': {e}")
        finally:
            if cursor and not cursor.closed:
                cursor.close()
            if path_local and os.path.exists(path_local):
                 try:
                     os.remove(path_local)
                 except OSError:
                     pass

    if client:
        client.close()
        print("Conexión a MongoDB cerrada.")

    print("Proceso de ingesta MongoDB finalizado.")

if __name__ == "__main__":
    run_ingestion()