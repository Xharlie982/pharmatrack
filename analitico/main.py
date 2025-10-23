import os
import time
import re
from typing import List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

# ===================== Configuración desde .env =====================
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DB: str = os.getenv("ATHENA_DB", "").strip()
ATHENA_OUTPUT: str = os.getenv("ATHENA_OUTPUT", "").strip()  

def _normalize_prefix(p: str) -> str:
    if not p:
        return ""
    p = p.strip()
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/")

API_PREFIX: str = _normalize_prefix(os.getenv("ANALITICO_BASE_PATH", "/analitico"))

_cors = os.getenv("CORS_ORIGINS", "*")
ALLOW_ORIGINS: List[str] = [o.strip() for o in _cors.split(",")] if _cors != "*" else ["*"]

if not ATHENA_DB:
    print("ADVERTENCIA: ATHENA_DB no está definido; las consultas fallarán.")
if not (ATHENA_OUTPUT.startswith("s3://") and ATHENA_OUTPUT.endswith("/")):
    print(f"ADVERTENCIA: ATHENA_OUTPUT inválido ('{ATHENA_OUTPUT}'). Debe empezar con s3:// y terminar con /.")

# ===================== Cliente Boto3 / Athena =====================
boto_config = Config(retries={"max_attempts": 5, "mode": "adaptive"})
session = boto3.Session(region_name=AWS_REGION)
athena_client = session.client("athena", config=boto_config)

# ===================== FastAPI (docs con barra final) =====================
app = FastAPI(
    title="API Analítica PharmaTrack (Athena)",
    version="1.0.0",
    docs_url=f"{API_PREFIX}/docs/" if API_PREFIX else "/docs/",   
    redoc_url=None,
    openapi_url=f"{API_PREFIX}/openapi.json" if API_PREFIX else "/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===================== Util: ejecutar consulta en Athena =====================
def run_athena_query(query: str, max_wait_seconds: int = 90):
    if not ATHENA_DB:
        raise HTTPException(status_code=500, detail="Configuración inválida: ATHENA_DB no está definido.")
    if not (ATHENA_OUTPUT.startswith("s3://") and ATHENA_OUTPUT.endswith("/")):
        raise HTTPException(status_code=500, detail="Configuración inválida: ATHENA_OUTPUT debe iniciar con s3:// y terminar con /.")

    try:
        print(f"[Athena] DB='{ATHENA_DB}' :: {query[:300]}...")
        resp = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": ATHENA_DB},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        qid = resp["QueryExecutionId"]

        state = "QUEUED"
        elapsed, poll = 0, 1
        while state in ("QUEUED", "RUNNING") and elapsed < max_wait_seconds:
            ex = athena_client.get_query_execution(QueryExecutionId=qid)
            state = ex["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = ex["QueryExecution"]["Status"].get("StateChangeReason", "Error desconocido")
                raise HTTPException(status_code=500, detail=f"Error en consulta Athena: {state} - {reason}")
            time.sleep(poll)
            elapsed += poll
            if elapsed > 10: poll = 2
            if elapsed > 30: poll = 5

        if state != "SUCCEEDED":
            raise HTTPException(status_code=500, detail=f"Consulta Athena no completada: {state}. Timeout {max_wait_seconds}s")

        paginator = athena_client.get_paginator("get_query_results")
        pages = paginator.paginate(QueryExecutionId=qid, PaginationConfig={"PageSize": 1000})

        results = []
        cols = []
        first = True
        for page in pages:
            rs = page["ResultSet"]
            if first:
                cols = [c["Name"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
                rows = rs.get("Rows", [])[1:]  
                first = False
            else:
                rows = rs.get("Rows", [])
            for r in rows:
                values = [cell.get("VarCharValue") for cell in r.get("Data", [])]
                if len(values) == len(cols):
                    results.append(dict(zip(cols, values)))
        return results

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        msg = e.response.get("Error", {}).get("Message", str(e))
        if code == "InvalidRequestException":
            raise HTTPException(status_code=400, detail=f"InvalidRequest: {msg}")
        if code == "AccessDeniedException":
            raise HTTPException(status_code=403, detail=f"AccessDenied: {msg}")
        raise HTTPException(status_code=500, detail=f"Error de AWS API: {code} - {msg}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {type(e).__name__}: {e}")

# ===================== Redirecciones a /docs/ =====================
if API_PREFIX:
    @app.get(API_PREFIX, include_in_schema=False)
    async def redirect_prefix_no_slash(_: Request):
        return RedirectResponse(url=app.docs_url, status_code=307)

    @app.get(f"{API_PREFIX}/", include_in_schema=False)
    async def redirect_prefix_slash(_: Request):
        return RedirectResponse(url=app.docs_url, status_code=307)
else:
    @app.get("/", include_in_schema=False)
    async def redirect_root(_: Request):
        return RedirectResponse(url=app.docs_url, status_code=307)

# ===================== Health =====================
@app.get(f"{API_PREFIX}/healthz", summary="Health Check", tags=["Health"])
async def healthz():
    return {"status": "ok"}

@app.get(f"{API_PREFIX}/ping_athena", summary="Quick Athena ping", tags=["Health"])
def ping_athena():
    return run_athena_query("SELECT 1 AS ok")

# ===================== Vistas =====================
@app.get(f"{API_PREFIX}/vista/stock_bajo", summary="Productos con Bajo Stock", tags=["Vistas"])
async def get_vista_stock_bajo():
    query = "SELECT * FROM vista_stock_bajo_reposicion ORDER BY cantidad_a_reponer DESC;"
    res = run_athena_query(query)
    for item in res:
        for k in ("stock_actual", "umbral_reposicion", "cantidad_a_reponer"):
            v = item.get(k)
            if v is None:
                continue
            try:
                item[k] = int(v)
            except (ValueError, TypeError):
                try:
                    item[k] = float(v)
                except (ValueError, TypeError):
                    pass
    return res

@app.get(f"{API_PREFIX}/vista/productos_mas_recetados", summary="Top Productos Más Recetados", tags=["Vistas"])
async def get_vista_productos_mas_recetados(
    limit: int = Query(10, ge=1, le=200, description="Número de productos a retornar (1-200)")
):
    query = f"SELECT * FROM vista_productos_mas_recetados LIMIT {limit};"
    res = run_athena_query(query)
    for item in res:
        v = item.get("total_recetado")
        try:
            item["total_recetado"] = int(v) if v is not None else 0
        except (ValueError, TypeError):
            pass
    return res

# ===================== KPIs =====================
@app.get(f"{API_PREFIX}/kpi/stockout", summary="Alerta de Quiebre de Stock", tags=["KPIs"])
async def kpi_stockout(distrito: Optional[str] = Query(None, description="Filtrar por distrito (texto simple)")):
    where = ""
    if distrito:
        if re.fullmatch(r"^[a-zA-Z0-9_ ]+$", distrito):
            distrito_esc = distrito.replace("'", "''")
            where = f"WHERE s.distrito = '{distrito_esc}'"
        else:
            raise HTTPException(status_code=400, detail="Formato de distrito inválido.")

    query = f"""
    SELECT 
        s.distrito,
        st.id_producto,
        p.nombre AS nombre_producto,
        SUM(st.stock_actual) AS stock_total_distrito,
        MIN(st.umbral_reposicion) AS umbral_reposicion,
        CASE WHEN SUM(st.stock_actual) <= MIN(st.umbral_reposicion) THEN true ELSE false END AS en_alerta
    FROM stock st
    JOIN sucursal s ON s.id_sucursal = st.id_sucursal
    JOIN productos p ON st.id_producto = p."_id"
    {where}
    GROUP BY s.distrito, st.id_producto, p.nombre
    ORDER BY en_alerta DESC, s.distrito, stock_total_distrito ASC
    """
    res = run_athena_query(query)
    for item in res:
        for k in ("stock_total_distrito", "umbral_reposicion"):
            v = item.get(k)
            try:
                item[k] = int(v) if v is not None else 0
            except (ValueError, TypeError):
                item[k] = 0
        item["en_alerta"] = str(item.get("en_alerta", "false")).lower() == "true"
    return res

@app.get(f"{API_PREFIX}/kpi/cobertura", summary="Días de Cobertura de Stock por Producto/Sucursal", tags=["KPIs"])
async def kpi_cobertura():
    query = """
    WITH demanda_diaria_promedio AS (
        SELECT 
            r.id_sucursal,
            d.id_producto,
            CAST(SUM(d.cantidad) AS double) / 30.0 AS demanda_promedio_diaria
        FROM receta r
        JOIN receta_detalle d ON r.id_receta = d.id_receta
        WHERE TRY_CAST(r.fecha_receta AS date) >= date_add('day', -30, current_date)
        GROUP BY r.id_sucursal, d.id_producto
    )
    SELECT 
        st.id_sucursal,
        s.nombre AS nombre_sucursal,
        st.id_producto,
        p.nombre AS nombre_producto,
        st.stock_actual,
        COALESCE(ddp.demanda_promedio_diaria, 0.0) AS demanda_promedio_diaria,
        CASE WHEN COALESCE(ddp.demanda_promedio_diaria, 0.0) > 0.0
             THEN CAST(st.stock_actual AS double) / ddp.demanda_promedio_diaria
             ELSE NULL END AS dias_cobertura_estimados
    FROM stock st
    JOIN sucursal s ON st.id_sucursal = s.id_sucursal
    JOIN productos p ON st.id_producto = p."_id"
    LEFT JOIN demanda_diaria_promedio ddp
           ON ddp.id_sucursal = st.id_sucursal AND ddp.id_producto = st.id_producto
    ORDER BY dias_cobertura_estimados ASC NULLS FIRST, st.id_sucursal, st.id_producto
    """
    res = run_athena_query(query)
    for item in res:
        try:
            item["id_sucursal"] = int(item.get("id_sucursal") or 0)
        except (ValueError, TypeError):
            item["id_sucursal"] = 0
        try:
            item["stock_actual"] = int(item.get("stock_actual") or 0)
        except (ValueError, TypeError):
            item["stock_actual"] = 0
        try:
            item["demanda_promedio_diaria"] = float(item.get("demanda_promedio_diaria") or 0.0)
        except (ValueError, TypeError):
            item["demanda_promedio_diaria"] = 0.0
        v = item.get("dias_cobertura_estimados")
        try:
            item["dias_cobertura_estimados"] = float(v) if v is not None else None
        except (ValueError, TypeError):
            item["dias_cobertura_estimados"] = None
    return res