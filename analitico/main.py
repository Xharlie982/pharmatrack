import os
import time
from typing import List, Dict, Any, Optional

import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse


# ===================== Config =====================
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DB: str = os.getenv("ATHENA_DB", "pharmatrack_raw")
ATHENA_OUTPUT: str = os.getenv("ATHENA_OUTPUT", "s3://resultados-athena-g6/")

# BASE_PATH viene de tu .env como ANALITICO_BASE_PATH=/analitico
_raw_prefix = (os.getenv("BASE_PATH", "")).strip()
def _normalize_prefix(p: str) -> str:
    if not p:
        return ""
    p = p.strip()
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/")

BASE_PATH: str = _normalize_prefix(_raw_prefix)

_cors = os.getenv("CORS_ORIGINS", "*")
ALLOW_ORIGINS: List[str] = [o.strip() for o in _cors.split(",")] if _cors else ["*"]

athena = boto3.client(
    "athena",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 5, "mode": "adaptive"})
)

# root_path hace que FastAPI genere rutas (docs, openapi) con el prefijo del ALB
app = FastAPI(
    title="API Analítica (Athena)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
    root_path=BASE_PATH or ""
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===================== Helpers =====================
def run_query(sql: str) -> List[Dict[str, Any]]:
    """
    Ejecuta una consulta en Athena y retorna una lista de dicts (column -> valor).
    Requiere que ATHENA_OUTPUT apunte a un bucket/prefijo válido.
    """
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    qid = q["QueryExecutionId"]

    # Espera activa simple
    while True:
        qe = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if qe in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.6)

    if qe != "SUCCEEDED":
        raise HTTPException(status_code=500, detail=f"Athena {qe}")

    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results.get("ResultSet", {}).get("Rows", [])
    if not rows:
        return []

    headers = [c.get("VarCharValue") for c in rows[0].get("Data", [])]
    def row_to_dict(row) -> Dict[str, Any]:
        data = row.get("Data", [])
        out: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            out[h] = data[i].get("VarCharValue") if i < len(data) and h is not None else None
        return out

    out: List[Dict[str, Any]] = [row_to_dict(r) for r in rows[1:]]
    next_token = results.get("NextToken")
    while next_token:
        results = athena.get_query_results(QueryExecutionId=qid, NextToken=next_token)
        for r in results.get("ResultSet", {}).get("Rows", []):
            out.append(row_to_dict(r))
        next_token = results.get("NextToken")
    return out


# ===================== Rutas =====================
@app.get("/")
def root_redirect():
    # Si accedes a http://<host>:8086/analitico → redirige a /analitico/docs
    target = (BASE_PATH or "") + "/docs"
    return RedirectResponse(url=target, status_code=302)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/kpi/fill-rate")
def kpi_fill_rate(desde: Optional[str] = None,
                  hasta: Optional[str] = None,
                  distrito: Optional[str] = None):
    where = []
    if desde: where.append(f"date(r.fecha_receta) >= date('{desde}')")
    if hasta: where.append(f"date(r.fecha_receta) <= date('{hasta}')")
    if distrito: where.append(f"s.distrito = '{distrito}'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
    SELECT date_trunc('day', r.fecha_receta) AS dia, s.distrito,
           SUM(d.cantidad) AS recetado,
           SUM(coalesce(x.cantidad_total,0)) AS dispensado,
           SUM(coalesce(x.cantidad_total,0))/NULLIF(SUM(d.cantidad),0) AS fill_rate
    FROM receta r
    JOIN receta_detalle d ON r.id_receta=d.id_receta
    JOIN sucursal s ON s.id_sucursal=r.id_sucursal
    LEFT JOIN dispensacion x ON x.id_receta=r.id_receta
    {where_sql}
    GROUP BY 1,2
    ORDER BY 1,2
    """
    return run_query(sql)

@app.get("/kpi/stockout")
def kpi_stockout(distrito: Optional[str] = None):
    where = [f"s.distrito = '{distrito}'"] if distrito else []
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
    SELECT s.distrito, st.id_producto,
           SUM(st.stock_actual) AS stock_actual,
           MIN(st.umbral_reposicion) AS umbral_reposicion,
           CASE WHEN SUM(st.stock_actual) <= MIN(st.umbral_reposicion) THEN 1 ELSE 0 END AS alerta
    FROM stock st
    JOIN sucursal s ON s.id_sucursal=st.id_sucursal
    {where_sql}
    GROUP BY s.distrito, st.id_producto
    ORDER BY alerta DESC, distrito
    """
    return run_query(sql)

@app.get("/top/quiebres")
def top_quiebres(limite: int = 20):
    sql = f"""
    SELECT p.codigo_atc, p.nombre, COUNT(*) AS dias_en_alerta
    FROM v_alertas_stockout_diario a
    JOIN catalogo_producto p ON p.id_producto=a.id_producto
    WHERE a.dia >= date_add('day', -7, current_date)
    GROUP BY p.codigo_atc, p.nombre
    ORDER BY dias_en_alerta DESC
    LIMIT {limite}
    """
    return run_query(sql)

@app.get("/kpi/cobertura")
def kpi_cobertura():
    sql = """
    WITH demanda AS (
      SELECT s.id_sucursal, d.id_producto, AVG(d.cantidad) AS demanda_diaria
      FROM receta r
      JOIN receta_detalle d ON r.id_receta=d.id_receta
      JOIN sucursal s ON s.id_sucursal=r.id_sucursal
      WHERE r.fecha_receta >= date_add('day', -30, current_date)
      GROUP BY s.id_sucursal, d.id_producto
    )
    SELECT st.id_sucursal, st.id_producto, st.stock_actual, d.demanda_diaria,
           CASE WHEN d.demanda_diaria>0 THEN st.stock_actual/d.demanda_diaria ELSE NULL END AS dias_cobertura
    FROM stock st
    LEFT JOIN demanda d ON d.id_sucursal=st.id_sucursal AND d.id_producto=st.id_producto
    ORDER BY dias_cobertura ASC NULLS FIRST
    """
    return run_query(sql)