import os
from datetime import datetime, date, timezone
from typing import List, Optional, Annotated
from enum import Enum as PyEnum
import httpx

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Integer, String, DateTime, Enum, ForeignKey, select
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Mapped, mapped_column

# =============== Config ===============
def _as_bool(v: Optional[str], default: bool = True) -> bool:
    if v is None: return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y")

BASE_PATH = (os.getenv("RECETAS_BASE_PATH", "")).rstrip("/")

DB_URL = os.getenv("MYSQL_URL")

CATALOGO_BASE_URL = os.getenv("CATALOGO_BASE_URL")
INVENTARIO_BASE_URL = os.getenv("INVENTARIO_BASE_URL")

VALIDATE_PRODUCTO = _as_bool(os.getenv("VALIDATE_PRODUCTO", "1"), True)
VALIDATE_SUCURSAL = _as_bool(os.getenv("VALIDATE_SUCURSAL", "1"), True)

FAIL_CLOSED = _as_bool(os.getenv("FAIL_CLOSED", "1"), True)

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "3.0"))
http_client = httpx.Client(timeout=HTTP_TIMEOUT)

def to_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None: return None
    if dt.tzinfo is not None: return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt
def date_to_start_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

# =============== DB ===============
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
Base = declarative_base()
class EstadoReceta(str, PyEnum):
    NUEVA = "NUEVA"; VALIDADA = "VALIDADA"; DISPENSADA = "DISPENSADA"; ANULADA = "ANULADA"
class Receta(Base):
    __tablename__ = "receta"; id_receta: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True); id_sucursal: Mapped[int] = mapped_column(Integer, nullable=False); nombre_paciente: Mapped[Optional[str]] = mapped_column(String(100)); fecha_receta: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.utcnow(), nullable=False); estado: Mapped[str] = mapped_column(Enum(*[e.value for e in EstadoReceta], name="estado_receta"), default=EstadoReceta.NUEVA.value, nullable=False); detalle: Mapped[List["RecetaDetalle"]] = relationship(backref="receta", cascade="all,delete", passive_deletes=True)
class RecetaDetalle(Base):
    __tablename__ = "receta_detalle"; id_receta: Mapped[int] = mapped_column(ForeignKey("receta.id_receta", ondelete="CASCADE"), primary_key=True); id_producto: Mapped[str] = mapped_column(String(64), primary_key=True); cantidad: Mapped[int] = mapped_column(Integer, nullable=False)
class Dispensacion(Base):
    __tablename__ = "dispensacion"; id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True); id_receta: Mapped[int] = mapped_column(ForeignKey("receta.id_receta", ondelete="RESTRICT"), nullable=False); fecha_dispensacion: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.utcnow(), nullable=False); cantidad_total: Mapped[Optional[int]] = mapped_column(Integer)
Base.metadata.create_all(engine)

# ===== FastAPI =====
app = FastAPI(
    title="Recetas y Dispensaciones API", version="1.2.0", description="Microservicio para la gestión de recetas médicas y su dispensación.",
    docs_url=f"{BASE_PATH}/docs" if BASE_PATH else "/docs", redoc_url=None, openapi_url=f"{BASE_PATH}/openapi.json" if BASE_PATH else "/openapi.json",
)
app.add_middleware(CORSMiddleware, allow_origins=(os.getenv("CORS_ORIGINS", "*")).split(","), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
@app.on_event("shutdown")
def _shutdown():
    try: http_client.close()
    except Exception: pass

# =============== Validaciones Externas ===============
def validar_producto(id_producto: str):
    if not VALIDATE_PRODUCTO: return
    try:
        r = http_client.get(f"{CATALOGO_BASE_URL}/productos/{id_producto}")
        r.raise_for_status()
        data = r.json()
        if not data.get("activo"): raise HTTPException(status_code=409, detail=f"Operación rechazada: El producto '{id_producto}' está inactivo.")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404: raise HTTPException(status_code=404, detail=f"Producto '{id_producto}' no encontrado en Catálogo.") from e
        raise HTTPException(status_code=503, detail=f"Error inesperado de Catálogo: {e.response.status_code}") from e
    except httpx.RequestError as e:
        if FAIL_CLOSED: raise HTTPException(status_code=503, detail=f"Servicio de Catálogo no disponible: {e}") from e

def validar_sucursal(id_sucursal: int):
    if not VALIDATE_SUCURSAL: return
    try:
        r = http_client.get(f"{INVENTARIO_BASE_URL}/sucursales/{id_sucursal}")
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404: raise HTTPException(status_code=404, detail=f"Sucursal '{id_sucursal}' no encontrada en Inventario.") from e
        raise HTTPException(status_code=503, detail=f"Error inesperado de Inventario: {e.response.status_code}") from e
    except httpx.RequestError as e:
        if FAIL_CLOSED: raise HTTPException(status_code=503, detail=f"Servicio de Inventario no disponible: {e}") from e

# =============== Schemas (Pydantic) ===============
class RecetaCreate(BaseModel): id_sucursal: int; nombre_paciente: Optional[str] = None
class LineaCreate(BaseModel): id_producto: str = Field(min_length=1); cantidad: int = Field(gt=0)
class DispensacionCreate(BaseModel): id_receta: int; cantidad_total: Optional[int] = Field(default=None, ge=0)

# =============== Endpoints ===============
@app.get(f"{BASE_PATH}/", tags=["Info"], include_in_schema=False)
def root(request: Request):
    accept_header = request.headers.get("accept", "")
    if "text/html" in accept_header: return RedirectResponse(url=app.docs_url)
    return {"service": "recetas", "docs": app.docs_url, "health": f"{BASE_PATH}/healthz"}

@app.get(f"{BASE_PATH}/healthz", tags=["Health"])
def healthz(): return {"status": "ok"}

@app.post("/recetas", tags=["Recetas"], status_code=201)
def crear_receta(body: RecetaCreate):
    validar_sucursal(body.id_sucursal)
    with Session() as s:
        r = Receta(id_sucursal=body.id_sucursal, nombre_paciente=body.nombre_paciente)
        s.add(r); s.commit(); s.refresh(r)
        return r

@app.get("/recetas/{id_receta}", tags=["Recetas"])
def obtener_receta(id_receta: int):
    with Session() as s:

        receta = s.get(Receta, id_receta)
        if not receta:
            raise HTTPException(404, "Receta no encontrada")

        detalles = s.query(RecetaDetalle).filter_by(id_receta=id_receta).all()

        return {
            "id_receta": receta.id_receta,
            "id_sucursal": receta.id_sucursal,
            "nombre_paciente": receta.nombre_paciente,
            "fecha_receta": receta.fecha_receta,
            "estado": receta.estado,
            "detalle": [{"id_producto": d.id_producto, "cantidad": d.cantidad} for d in detalles]
        }

@app.post("/recetas/{id_receta}/detalle", tags=["Recetas"])
def agregar_linea(id_receta: int, body: LineaCreate):
    validar_producto(body.id_producto)
    with Session() as s:
        if not s.get(Receta, id_receta): raise HTTPException(404, "Receta no existe")
        existing = s.get(RecetaDetalle, (id_receta, body.id_producto))
        if existing:
            existing.cantidad = body.cantidad
            s.commit()
            return JSONResponse({"ok": True, "message": "Línea actualizada"}, status_code=200)
        d = RecetaDetalle(id_receta=id_receta, id_producto=body.id_producto, cantidad=body.cantidad)
        s.add(d); s.commit()
        return JSONResponse({"ok": True, "message": "Línea creada"}, status_code=201)

@app.get("/recetas", tags=["Recetas"])
def listar_recetas(estado: Optional[EstadoReceta] = None, desde: Optional[datetime | date] = None, hasta: Optional[datetime | date] = None):
    d_desde = None; d_hasta = None
    if isinstance(desde, date) and not isinstance(desde, datetime): d_desde = date_to_start_utc(desde)
    elif isinstance(desde, datetime): d_desde = to_utc_naive(desde)
    if isinstance(hasta, date) and not isinstance(hasta, datetime): d_hasta = date_to_start_utc(hasta)
    elif isinstance(hasta, datetime): d_hasta = to_utc_naive(hasta)
    with Session() as s:
        q = select(Receta)
        if estado: q = q.where(Receta.estado == estado.value)
        if d_desde: q = q.where(Receta.fecha_receta >= d_desde)
        if d_hasta: q = q.where(Receta.fecha_receta <= d_hasta)
        return s.scalars(q.order_by(Receta.id_receta.desc())).all()

@app.post("/dispensaciones", tags=["Dispensaciones"], status_code=201)
def registrar_dispensacion(body: DispensacionCreate):
    with Session() as s:
        r = s.get(Receta, body.id_receta)
        if not r: raise HTTPException(404, "Receta no existe")
        x = Dispensacion(id_receta=body.id_receta, cantidad_total=body.cantidad_total)
        s.add(x)
        if r.estado != EstadoReceta.DISPENSADA.value:
            r.estado = EstadoReceta.DISPENSADA.value
        s.commit(); s.refresh(x)
        return x