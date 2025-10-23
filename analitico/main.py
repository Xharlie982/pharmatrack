import os
import time
import uuid
from typing import List, Dict, Any, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from fastapi.openapi.utils import get_openapi

# ===================== Config =====================
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DB: str = os.getenv("ATHENA_DB", "pharmatrack_analytics_db") 
ATHENA_OUTPUT: str = os.getenv("ATHENA_OUTPUT", "s3://default-bucket/") 

_raw_prefix = os.getenv("ANALITICO_BASE_PATH", "").strip() 

def _normalize_prefix(p: str) -> str:
    if not p: return ""
    p = p.strip()
    if not p.startswith("/"): p = "/" + p
    return p.rstrip("/")

BASE_PATH: str = _normalize_prefix(_raw_prefix)

_cors = os.getenv("CORS_ORIGINS", "*")
ALLOW_ORIGINS: List[str] = [o.strip() for o in _cors.split(",")] if _cors != "*" else ["*"]

if not ATHENA_OUTPUT or not ATHENA_OUTPUT.startswith("s3://") or not ATHENA_OUTPUT.endswith("/"):
    print(f"¡ADVERTENCIA CRÍTICA! ATHENA_OUTPUT ('{ATHENA_OUTPUT}') no es válido. Debe empezar con s3:// y terminar con /. Las consultas fallarán.")

# --- Cliente Boto3 ---
boto_config = Config(
    retries={
        'max_attempts': 5,
        'mode': 'adaptive'
    }
)
session = boto3.Session(region_name=AWS_REGION)
athena_client = session.client('athena', config=boto_config)


# --- Aplicación FastAPI ---
app = FastAPI(
    title="API Analítica PharmaTrack (Athena)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None, 
    openapi_url="/openapi.json",
    root_path=BASE_PATH 
)

# ---- Configuración para Ocultar "Servers" en Swagger ----
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
        description=app.description,
    )
    openapi_schema.pop("servers", None)
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
# --------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===================== Función Auxiliar Athena =====================

def run_athena_query(query: str, max_wait_seconds: int = 90):
    """
    Ejecuta una consulta en Athena, espera a que termine y devuelve los resultados 
    como una lista de diccionarios.
    """
    if not ATHENA_OUTPUT or not ATHENA_OUTPUT.startswith("s3://") or not ATHENA_OUTPUT.endswith("/"):
         raise HTTPException(status_code=500, detail="Configuración inválida: ATHENA_OUTPUT no está bien definido.")
         
    try:
        print(f"Ejecutando consulta en Athena DB '{ATHENA_DATABASE}':\n{query[:500]}...") 
        
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        query_execution_id = response['QueryExecutionId']
        print(f"Athena Query Execution ID: {query_execution_id}")

        state = 'QUEUED'
        elapsed_time = 0
        poll_interval = 1 
        
        while state in ['QUEUED', 'RUNNING'] and elapsed_time < max_wait_seconds:
            try:
                execution_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                state = execution_response['QueryExecution']['Status']['State']
                print(f"Query state: {state} ({elapsed_time}s)")

                if state == 'SUCCEEDED':
                    break
                elif state in ['FAILED', 'CANCELLED']:
                    error_message = execution_response['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido de Athena.')
                    print(f"Error en consulta Athena. Estado: {state}, Razón: {error_message}")
                    raise HTTPException(status_code=500, detail=f"Error en consulta Athena: {state} - {error_message}")
                
                time.sleep(poll_interval)
                elapsed_time += poll_interval
                if elapsed_time > 10: poll_interval = 2
                if elapsed_time > 30: poll_interval = 5

            except ClientError as ce:
                 print(f"Error ClientError al verificar estado de consulta: {ce}")
                 if ce.response['Error']['Code'] == 'ThrottlingException':
                      print("Throttling detectado, esperando más tiempo...")
                      time.sleep(poll_interval * 2) 
                      elapsed_time += poll_interval * 2
                 else:
                      raise 

        if state != 'SUCCEEDED':
            print(f"Consulta Athena no completada exitosamente o superó el tiempo de espera ({max_wait_seconds}s). Estado final: {state}")
            raise HTTPException(status_code=500, detail=f"Consulta Athena no completada: {state}. Timeout: {max_wait_seconds}s")

        print("Consulta exitosa. Obteniendo resultados...")
        results_paginator = athena_client.get_paginator('get_query_results')
        results_iter = results_paginator.paginate(
            QueryExecutionId=query_execution_id,
            PaginationConfig={'PageSize': 1000}
        )

        results = []
        column_names = []
        is_first_page = True

        for page in results_iter:
            if is_first_page:
                if 'ResultSetMetadata' not in page['ResultSet']:
                     print("Advertencia: ResultSetMetadata no encontrado en la primera página.")
                     return [] 
                column_info = page['ResultSet']['ResultSetMetadata']['ColumnInfo']
                column_names = [col['Name'] for col in column_info]
                rows_to_process = page['ResultSet']['Rows'][1:] 
                is_first_page = False
            else:
                 rows_to_process = page['ResultSet']['Rows']
                 
            if not column_names:
                 print("Error: No se pudieron determinar los nombres de columna.")
                 raise HTTPException(status_code=500, detail="Error procesando resultados de Athena: Faltan nombres de columna.")

            for row in rows_to_process:
                values = [item.get('VarCharValue', None) for item in row.get('Data', [])]
                if len(values) == len(column_names):
                    results.append(dict(zip(column_names, values)))
                else:
                     print(f"Advertencia: Discrepancia en número de columnas y valores en la fila: {row.get('Data', [])}")

        print(f"Resultados obtenidos: {len(results)} filas.")
        return results

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        print(f"Error Boto3 ClientError interactuando con Athena: {error_code} - {error_message}")
        if error_code == 'InvalidRequestException':
             raise HTTPException(status_code=400, detail=f"Error en la consulta Athena (InvalidRequest): {error_message}")
        elif error_code == 'AccessDeniedException':
             raise HTTPException(status_code=403, detail=f"Acceso denegado a Athena o S3: {error_message}")
        else:
             raise HTTPException(status_code=500, detail=f"Error de AWS API: {error_message}")
    except HTTPException as httpe:
         raise httpe 
    except Exception as e:
        print(f"Error inesperado ejecutando consulta Athena: {type(e).__name__} - {str(e)}")
        import traceback
        traceback.print_exc() 
        raise HTTPException(status_code=500, detail=f"Error interno del servidor al procesar la consulta: {str(e)}")


# ===================== Rutas API =====================

@app.get("/", include_in_schema=False)
async def root_redirect():
    target_path = "/docs"
    redirect_url = BASE_PATH + target_path if BASE_PATH else target_path
    if not BASE_PATH: redirect_url = target_path
    print(f"Redirigiendo desde la raíz (relativa a '{BASE_PATH}') a '{redirect_url}'")
    return RedirectResponse(url=redirect_url, status_code=307)



@app.get("/healthz", summary="Health Check")
async def healthz():
    """Endpoint simple para verificar que la API está respondiendo."""
    return {"status": "ok"}


@app.get("/vista/stock_bajo", summary="Productos con Bajo Stock")
async def get_vista_stock_bajo():
    """
    Consulta la vista 'vista_stock_bajo_reposicion' para obtener productos
    cuyo stock actual está por debajo del umbral definido.
    """
    query = 'SELECT * FROM vista_stock_bajo_reposicion ORDER BY cantidad_a_reponer DESC;'
    try:
        results = run_athena_query(query)

        for item in results:
            for key in ['stock_actual', 'umbral_reposicion', 'cantidad_a_reponer']:
                if item.get(key) is not None:
                    try: item[key] = int(item[key])
                    except (ValueError, TypeError):
                         try: item[key] = float(item[key])
                         except (ValueError, TypeError): pass 
        return results
    except HTTPException as e: raise e
    except Exception as e:
        print(f"Error inesperado en endpoint /vista/stock_bajo: {e}")
        raise HTTPException(status_code=500, detail="Error interno procesando stock bajo.")

@app.get("/vista/productos_mas_recetados", summary="Top Productos Más Recetados")
async def get_vista_productos_mas_recetados(
    limit: int = Query(10, ge=1, le=200, description="Número de productos a retornar (1-200)") # <-- LÍMITE CAMBIADO A 200
):
    """
    Consulta la vista 'vista_productos_mas_recetados' para obtener los N
    productos más recetados globalmente.
    """
    query = f'SELECT * FROM vista_productos_mas_recetados LIMIT {limit};' 
    try:
        results = run_athena_query(query)

        for item in results:
             if item.get('total_recetado') is not None:
                 try: item['total_recetado'] = int(item['total_recetado'])
                 except (ValueError, TypeError): pass 
        return results
    except HTTPException as e: raise e
    except Exception as e:
        print(f"Error inesperado en endpoint /vista/productos_mas_recetados: {e}")
        raise HTTPException(status_code=500, detail="Error interno procesando top productos recetados.")


# --- Endpoints basados en Consultas Directas (KPIs) ---

@app.get("/kpi/stockout", summary="Alerta de Quiebre de Stock")
async def kpi_stockout(
    distrito: Optional[str] = Query(None, description="Filtrar por distrito (sensible a mayúsculas)")
):
    """
    Identifica productos en riesgo de quiebre de stock (stock <= umbral)
    agrupado por distrito y producto.
    """
    where_clauses = []
    if distrito:
        import re
        if re.fullmatch(r"^[a-zA-Z0-9_ ]+$", distrito):
             where_clauses.append(f"s.distrito = '{distrito}'") 
        else:
             raise HTTPException(status_code=400, detail="Formato de distrito inválido.")
             
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    
    query = f"""
    SELECT 
        s.distrito, 
        st.id_producto,
        p.nombre AS nombre_producto, 
        SUM(st.stock_actual) AS stock_total_distrito,
        MIN(st.umbral_reposicion) AS umbral_reposicion, 
        CASE 
            WHEN SUM(st.stock_actual) <= MIN(st.umbral_reposicion) THEN true 
            ELSE false 
        END AS en_alerta
    FROM 
        stock st
    JOIN 
        sucursal s ON s.id_sucursal = st.id_sucursal
    JOIN
        productos p ON st.id_producto = p."_id" 
    {where_sql}
    GROUP BY 
        s.distrito, st.id_producto, p.nombre 
    ORDER BY 
        en_alerta DESC, s.distrito, stock_total_distrito ASC
    """
    try:
        results = run_athena_query(query)
        for item in results:
            for key in ['stock_total_distrito', 'umbral_reposicion']:
                 item[key] = int(item.get(key) or 0)
            item['en_alerta'] = item.get('en_alerta', 'false').lower() == 'true' 
        return results
    except HTTPException as e: raise e
    except Exception as e:
        print(f"Error inesperado en endpoint /kpi/stockout: {e}")
        raise HTTPException(status_code=500, detail="Error interno procesando alertas de stockout.")


@app.get("/kpi/cobertura", summary="Días de Cobertura de Stock por Producto/Sucursal")
async def kpi_cobertura():
    """
    Calcula los días de cobertura de stock para cada producto en cada sucursal,
    basado en la demanda promedio diaria (recetas) de los últimos 30 días.
    """
    query = """
    WITH demanda_diaria_promedio AS (
        SELECT 
            r.id_sucursal, 
            d.id_producto, 
            CAST(SUM(d.cantidad) AS double) / 30.0 AS demanda_promedio_diaria
        FROM 
            receta r
        JOIN 
            receta_detalle d ON r.id_receta = d.id_receta
        WHERE
            TRY_CAST(r.fecha_receta AS date) >= date_add('day', -30, current_date) 
        GROUP BY 
            r.id_sucursal, d.id_producto
    )
    SELECT 
        st.id_sucursal,
        s.nombre AS nombre_sucursal, 
        st.id_producto,
        p.nombre AS nombre_producto, 
        st.stock_actual, 
        COALESCE(ddp.demanda_promedio_diaria, 0.0) AS demanda_promedio_diaria,
        CASE 
            WHEN COALESCE(ddp.demanda_promedio_diaria, 0.0) > 0.0
            THEN CAST(st.stock_actual AS double) / ddp.demanda_promedio_diaria 
            ELSE NULL 
        END AS dias_cobertura_estimados
    FROM 
        stock st
    JOIN 
        sucursal s ON st.id_sucursal = s.id_sucursal 
    JOIN
        productos p ON st.id_producto = p."_id" 
    LEFT JOIN 
        demanda_diaria_promedio ddp ON ddp.id_sucursal = st.id_sucursal AND ddp.id_producto = st.id_producto
    ORDER BY 
        dias_cobertura_estimados ASC NULLS FIRST, 
        st.id_sucursal, 
        st.id_producto;
    """
    try:
        results = run_athena_query(query)
        for item in results:
            item['id_sucursal'] = int(item.get('id_sucursal') or 0)
            item['stock_actual'] = int(item.get('stock_actual') or 0)
            item['demanda_promedio_diaria'] = float(item.get('demanda_promedio_diaria') or 0.0)
            if item.get('dias_cobertura_estimados') is not None:
                try: item['dias_cobertura_estimados'] = float(item['dias_cobertura_estimados'])
                except (ValueError, TypeError): item['dias_cobertura_estimados'] = None 
            else: item['dias_cobertura_estimados'] = None
        return results
    except HTTPException as e: raise e
    except Exception as e:
        print(f"Error inesperado en endpoint /kpi/cobertura: {e}")
        raise HTTPException(status_code=500, detail="Error interno procesando cobertura de stock.")