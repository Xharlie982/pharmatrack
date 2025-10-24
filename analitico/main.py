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
from starlette.middleware.gzip import GZipMiddleware

# ===================== Config desde .env =====================
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
DOCS_URL = f"{API_PREFIX}/docs/" if API_PREFIX else "/docs/"
OPENAPI_URL = f"{API_PREFIX}/openapi.json" if API_PREFIX else "/openapi.json"

_cors = os.getenv("CORS_ORIGINS", "*")
ALLOW_ORIGINS: List[str] = [o.strip() for o in _cors.split(",")] if _cors != "*" else ["*"]

if not ATHENA_DB:
    print("ADVERTENCIA: ATHENA_DB no está definido.")
if not (ATHENA_OUTPUT.startswith("s3://") and ATHENA_OUTPUT.endswith("/")):
    print(f"ADVERTENCIA: ATHENA_OUTPUT inválido ('{ATHENA_OUTPUT}'). Debe iniciar con s3:// y terminar con /.")

# ===================== Utiles =====================
def sql_escape(value: str) -> str:
    """Escapa comillas simples para literales SQL."""
    return value.replace("'", "''")

def validate_simple_text(value: str, pattern: str = r"^[a-zA-Z0-9_ \-]+$") -> None:
    if not re.fullmatch(pattern, value):
        raise HTTPException(status_code=400, detail="Valor de filtro con formato inválido.")

# ===================== Boto3 / Athena =====================
boto_config = Config(retries={"max_attempts": 5, "mode": "adaptive"})
session = boto3.Session(region_name=AWS_REGION)
athena_client = session.client("athena", config=boto_config)

# ===================== FastAPI =====================
app = FastAPI(
    title="API Analítica PharmaTrack (Athena)",
    version="1.0.0",
    docs_url=DOCS_URL,  
    redoc_url=None,
    openapi_url=OPENAPI_URL,
)

app.add_middleware(GZipMiddleware, minimum_size=1024)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===================== Ejecutar consulta en Athena =====================
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
            if elapsed > 10:
                poll = 2
            if elapsed > 30:
                poll = 5

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
                if "ResultSetMetadata" not in rs:
                    return []
                cols = [c["Name"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
                rows = rs.get("Rows", [])[1:]  # omite header
                first = False
            else:
                rows = rs.get("Rows", [])
            if not cols:
                raise HTTPException(status_code=500, detail="Error procesando resultados: Faltan nombres de columna.")
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

@app.get(f"{API_PREFIX}/ping_athena", summary="Prueba rápida de conexión con Athena", tags=["Health"])
def ping_athena():
    return run_athena_query("SELECT 1 AS ok")

# ===================== Vistas (paginación sin OFFSET) =====================
@app.get(f"{API_PREFIX}/vista/stock_bajo", summary="Productos con Bajo Stock (paginado)", tags=["Vistas"])
async def get_vista_stock_bajo(
    page: int = Query(1, ge=1, description="Página (>=1)"),
    limit: int = Query(25, ge=1, le=500, description="Filas por página (1-500)"),
    distrito_o_sucursal: Optional[str] = Query(None, description="Filtra por 'sucursal' (nombre/zona)"),
    producto: Optional[str] = Query(None, description="Filtra por nombre de producto (contiene)"),
    solo_alerta: bool = Query(True, description="Solo filas con cantidad_a_reponer > 0"),
):
    filters: List[str] = []
    if distrito_o_sucursal:
        validate_simple_text(distrito_o_sucursal)
        suc = sql_escape(distrito_o_sucursal)
        filters.append(f"lower(sucursal) = lower('{suc}')")
    if producto:
        prod = sql_escape(producto.lower())
        filters.append(f"lower(producto) LIKE '%{prod}%'")
    if solo_alerta:
        filters.append("cantidad_a_reponer > 0")
    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""

    start = (page - 1) * limit + 1
    end = page * limit

    query = f"""
    WITH base AS (
      SELECT sucursal, producto, stock_actual, umbral_reposicion, cantidad_a_reponer
      FROM vista_stock_bajo_reposicion
      {where_sql}
    ),
    ranked AS (
      SELECT
        sucursal, producto, stock_actual, umbral_reposicion, cantidad_a_reponer,
        ROW_NUMBER() OVER (ORDER BY cantidad_a_reponer DESC, sucursal, producto) AS rn,
        COUNT(*) OVER () AS total_rows
      FROM base
    )
    SELECT * FROM ranked
    WHERE rn BETWEEN {start} AND {end}
    ORDER BY rn
    """
    rows = run_athena_query(query)

    data = []
    total = int(rows[0]["total_rows"]) if rows and rows[0].get("total_rows") else 0
    for item in rows:
        for k in ("stock_actual", "umbral_reposicion", "cantidad_a_reponer", "rn"):
            v = item.get(k)
            try:
                item[k] = int(v) if v is not None else 0
            except Exception:
                try:
                    item[k] = float(v)
                except Exception:
                    item[k] = 0
        data.append({
            "sucursal": item.get("sucursal"),
            "producto": item.get("producto"),
            "stock_actual": item.get("stock_actual"),
            "umbral_reposicion": item.get("umbral_reposicion"),
            "cantidad_a_reponer": item.get("cantidad_a_reponer"),
            "rownum": item.get("rn"),
        })

    return {"meta": {"page": page, "limit": limit, "total": total, "has_more": (page * limit) < total}, "data": data}

@app.get(f"{API_PREFIX}/vista/productos_mas_recetados", summary="Top Productos Más Recetados", tags=["Vistas"])
async def get_vista_productos_mas_recetados(
    limit: int = Query(10, ge=1, le=200, description="Número de productos a retornar (1-200)")
):
    rows = run_athena_query(f"SELECT * FROM vista_productos_mas_recetados LIMIT {limit}")
    for item in rows:
        v = item.get("total_recetado")
        try:
            item["total_recetado"] = int(v) if v is not None else 0
        except (ValueError, TypeError):
            pass
    return rows

# ===================== KPIs (paginados) =====================
@app.get(f"{API_PREFIX}/kpi/stockout", summary="Alerta de Quiebre de Stock (paginado)", tags=["KPIs"])
async def kpi_stockout(
    page: int = Query(1, ge=1, description="Página (>=1)"),
    limit: int = Query(50, ge=1, le=500, description="Filas por página (1-500)"),
    distrito: Optional[str] = Query(None, description="Filtrar por distrito"),
    producto: Optional[str] = Query(None, description="Filtrar por id/nombre producto (contiene)"),
    solo_alerta: bool = Query(True, description="Solo en alerta (stock_total <= umbral)"),
):
    filters: List[str] = []
    if distrito:
        validate_simple_text(distrito)
        dist_esc = sql_escape(distrito)
        filters.append(f"s.distrito = '{dist_esc}'")
    if producto:
        prod_esc = sql_escape(producto)
        filters.append(f"(CAST(st.id_producto AS varchar) ILIKE '%{prod_esc}%' OR p.nombre ILIKE '%{prod_esc}%')")

    where_sql = "WHERE " + " AND ".join(filters) if filters else ""

    start = (page - 1) * limit + 1
    end = page * limit

    query = f"""
    WITH agreg AS (
      SELECT 
        s.distrito,
        st.id_producto,
        p.nombre AS nombre_producto,
        SUM(st.stock_actual) AS stock_total_distrito,
        MIN(st.umbral_reposicion) AS umbral_reposicion
      FROM stock st
      JOIN sucursal s ON s.id_sucursal = st.id_sucursal
      JOIN productos p ON st.id_producto = p."_id"
      {where_sql}
      GROUP BY s.distrito, st.id_producto, p.nombre
    ),
    base AS (
      SELECT *,
             CASE WHEN stock_total_distrito <= umbral_reposicion THEN true ELSE false END AS en_alerta
      FROM agreg
    ),
    filtered AS (
      SELECT * FROM base
      {"WHERE en_alerta = true" if solo_alerta else ""}
    ),
    ranked AS (
      SELECT
        distrito, id_producto, nombre_producto,
        stock_total_distrito, umbral_reposicion, en_alerta,
        ROW_NUMBER() OVER (ORDER BY en_alerta DESC, distrito, stock_total_distrito ASC, nombre_producto) AS rn,
        COUNT(*) OVER () AS total_rows
      FROM filtered
    )
    SELECT * FROM ranked
    WHERE rn BETWEEN {start} AND {end}
    ORDER BY rn
    """
    rows = run_athena_query(query)

    data = []
    total = int(rows[0]["total_rows"]) if rows and rows[0].get("total_rows") else 0
    for item in rows:
        for k in ("stock_total_distrito", "umbral_reposicion", "rn"):
            v = item.get(k)
            try:
                item[k] = int(v) if v is not None else 0
            except Exception:
                item[k] = 0
        item["en_alerta"] = str(item.get("en_alerta", "false")).lower() == "true"
        data.append({
            "distrito": item.get("distrito"),
            "id_producto": item.get("id_producto"),
            "nombre_producto": item.get("nombre_producto"),
            "stock_total_distrito": item.get("stock_total_distrito"),
            "umbral_reposicion": item.get("umbral_reposicion"),
            "en_alerta": item.get("en_alerta"),
            "rownum": item.get("rn"),
        })

    return {"meta": {"page": page, "limit": limit, "total": total, "has_more": (page * limit) < total}, "data": data}

@app.get(f"{API_PREFIX}/kpi/cobertura", summary="Días de Cobertura por Producto/Sucursal (paginado)", tags=["KPIs"])
async def kpi_cobertura(
    page: int = Query(1, ge=1, description="Página (>=1)"),
    limit: int = Query(50, ge=1, le=500, description="Filas por página (1-500)"),
    distrito: Optional[str] = Query(None, description="Filtrar por distrito"),
    producto: Optional[str] = Query(None, description="Filtrar por id/nombre producto (contiene)"),
    demanda_positiva: bool = Query(False, description="Solo con demanda_promedio_diaria > 0"),
    min_dias: Optional[float] = Query(None, ge=0, description="Mínimo de días de cobertura"),
    max_dias: Optional[float] = Query(None, ge=0, description="Máximo de días de cobertura"),
):
    filters: List[str] = []
    if distrito:
        validate_simple_text(distrito)
        dist_esc = sql_escape(distrito)
        filters.append(f"s.distrito = '{dist_esc}'")
    if producto:
        prod_esc = sql_escape(producto)
        filters.append(f"(CAST(st.id_producto AS varchar) ILIKE '%{prod_esc}%' OR p.nombre ILIKE '%{prod_esc}%')")
    if demanda_positiva:
        filters.append("COALESCE(ddp.demanda_promedio_diaria, 0.0) > 0.0")
    if min_dias is not None:
        filters.append(
            " (CASE WHEN COALESCE(ddp.demanda_promedio_diaria, 0.0) > 0.0 "
            " THEN CAST(st.stock_actual AS double)/ddp.demanda_promedio_diaria ELSE NULL END) >= "
            f"{float(min_dias)}"
        )
    if max_dias is not None:
        filters.append(
            " (CASE WHEN COALESCE(ddp.demanda_promedio_diaria, 0.0) > 0.0 "
            " THEN CAST(st.stock_actual AS double)/ddp.demanda_promedio_diaria ELSE NULL END) <= "
            f"{float(max_dias)}"
        )

    where_sql = "WHERE " + " AND ".join(filters) if filters else ""

    start = (page - 1) * limit + 1
    end = page * limit

    query = f"""
    WITH demanda_diaria_promedio AS (
        SELECT 
            r.id_sucursal,
            d.id_producto,
            CAST(SUM(d.cantidad) AS double) / 30.0 AS demanda_promedio_diaria
        FROM receta r
        JOIN receta_detalle d ON r.id_receta = d.id_receta
        WHERE TRY_CAST(r.fecha_receta AS date) >= date_add('day', -30, current_date)
        GROUP BY r.id_sucursal, d.id_producto
    ),
    base AS (
        SELECT 
            st.id_sucursal,
            s.nombre AS nombre_sucursal,
            s.distrito AS distrito,
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
    ),
    filtered AS (
        SELECT * FROM base
        {where_sql}
    ),
    ranked AS (
        SELECT
            id_sucursal, nombre_sucursal, distrito,
            id_producto, nombre_producto,
            stock_actual, demanda_promedio_diaria, dias_cobertura_estimados,
            ROW_NUMBER() OVER (ORDER BY dias_cobertura_estimados ASC NULLS FIRST, id_sucursal, id_producto) AS rn,
            COUNT(*) OVER () AS total_rows
        FROM filtered
    )
    SELECT * FROM ranked
    WHERE rn BETWEEN {start} AND {end}
    ORDER BY rn
    """
    rows = run_athena_query(query)

    data = []
    total = int(rows[0]["total_rows"]) if rows and rows[0].get("total_rows") else 0
    for item in rows:
        for int_key in ("id_sucursal", "stock_actual", "rn"):
            try:
                item[int_key] = int(item.get(int_key) or 0)
            except Exception:
                item[int_key] = 0
        for flt_key in ("demanda_promedio_diaria", "dias_cobertura_estimados"):
            v = item.get(flt_key)
            try:
                item[flt_key] = float(v) if v is not None else None
            except Exception:
                item[flt_key] = None

        data.append({
            "id_sucursal": item.get("id_sucursal"),
            "nombre_sucursal": item.get("nombre_sucursal"),
            "distrito": item.get("distrito"),
            "id_producto": item.get("id_producto"),
            "nombre_producto": item.get("nombre_producto"),
            "stock_actual": item.get("stock_actual"),
            "demanda_promedio_diaria": item.get("demanda_promedio_diaria"),
            "dias_cobertura_estimados": item.get("dias_cobertura_estimados"),
            "rownum": item.get("rn"),
        })

    return {"meta": {"page": page, "limit": limit, "total": total, "has_more": (page * limit) < total}, "data": data}
