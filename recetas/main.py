import os
from datetime import datetime, date, timezone
from typing import List, Optional, Annotated
from enum import Enum as PyEnum

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pydantic import BaseModel, Field

from sqlalchemy import create_engine, Integer, String, DateTime, Enum, ForeignKey, select
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Mapped, mapped_column

import httpx

# =============== Config ===============
def _as_bool(v: Optional[str], default: bool = True) -> bool:
    if v is None: return default
    return v.strip().lower() in ("1","true","t","yes","y")

DB_URL = os.getenv("MYSQL_URL", "mysql+pymysql://user:pass@127.0.0.1:3306/recetas")
BASE_PATH = (os.getenv("BASE_PATH", "")).rstrip("/")   # ej: "/recetas"

CATALOGO_BASE_URL   = os.getenv("CATALOGO_BASE_URL",   "http://localhost:8084/catalogo").rstrip("/")
INVENTARIO_BASE_URL = os.getenv("INVENTARIO_BASE_URL", "http://localhost:8082/inventario").rstrip("/")

VALIDATE_PRODUCTO = _as_bool(os.getenv("VALIDATE_PRODUCTO", "1"), True)
VALIDATE_SUCURSAL = _as_bool(os.getenv("VALIDATE_SUCURSAL", "1"), True)
FAIL_CLOSED       = _as_bool(os.getenv("FAIL_CLOSED", "1"), True)
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", "3.0"))

http_client = httpx.Client(timeout=HTTP_TIMEOUT)

# Normaliza datetimes: a UTC y sin tzinfo (para compararlos con MySQL DATETIME)
def to_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None: return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt  # asumimos ya está en UTC naive

def date_to_start_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

# =============== DB ===============
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
Base = declarative_base()

class EstadoReceta(str, PyEnum):   # [Doc] Enum visible en Swagger
    NUEVA = "NUEVA"
    VALIDADA = "VALIDADA"
    DISPENSADA = "DISPENSADA"
    ANULADA = "ANULADA"

class Receta(Base):
    __tablename__ = "receta"
    id_receta: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_sucursal: Mapped[int] = mapped_column(Integer, nullable=False)
    nombre_paciente: Mapped[Optional[str]] = mapped_column(String(100))
    # guardamos UTC naive
    fecha_receta: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.utcnow(), nullable=False)
    estado: Mapped[str] = mapped_column(
        Enum(*[e.value for e in EstadoReceta], name="estado_receta"),
        default=EstadoReceta.NUEVA.value, nullable=False
    )
    detalle: Mapped[List["RecetaDetalle"]] = relationship(
        backref="receta", cascade="all,delete", passive_deletes=True
    )

class RecetaDetalle(Base):
    __tablename__ = "receta_detalle"
    id_receta: Mapped[int] = mapped_column(ForeignKey("receta.id_receta", ondelete="CASCADE"), primary_key=True)
    id_producto: Mapped[str] = mapped_column(String(64), primary_key=True)
    cantidad: Mapped[int] = mapped_column(Integer, nullable=False)

class Dispensacion(Base):
    __tablename__ = "dispensacion"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_receta: Mapped[int] = mapped_column(ForeignKey("receta.id_receta", ondelete="RESTRICT"), nullable=False)
    fecha_dispensacion: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.utcnow(), nullable=False)
    cantidad_total: Mapped[Optional[int]] = mapped_column(Integer)

Base.metadata.create_all(engine)

# =============== FastAPI ===============
docs_url    = f"{BASE_PATH}/docs" if BASE_PATH else "/docs"
openapi_url = f"{BASE_PATH}/openapi.json" if BASE_PATH else "/openapi.json"

app = FastAPI(
    title="PharmaTrack · Recetas (MySQL + FastAPI)",   # [Doc]
    version="1.1.0",                                    # [Doc]
    description=(
        "Registra **recetas**, sus **líneas** y **dispensaciones**.\n\n"
        "- Valida *id_sucursal* contra **Inventario**.\n"
        "- Valida *id_producto* contra **Catálogo**.\n"
        "- Fechas en **UTC**; filtros `desde`/`hasta` inclusive."
    ),  # [Doc]
    docs_url=docs_url, redoc_url=None, openapi_url=openapi_url
)

cors_origins = os.getenv("CORS_ORIGINS", "*")
origins = [o.strip() for o in cors_origins.split(",")] if cors_origins else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.on_event("shutdown")
def _shutdown():
    try: http_client.close()
    except Exception: pass

# =============== Validaciones externas ===============
def _exists_producto(id_producto: str) -> bool:
    if not VALIDATE_PRODUCTO: return True
    url = f"{CATALOGO_BASE_URL}/productos/{id_producto}"
    try:
        r = http_client.get(url)
        if r.status_code == 404: return False
        return 200 <= r.status_code < 300
    except httpx.RequestError as e:
        if FAIL_CLOSED:
            raise HTTPException(status_code=502, detail=f"Catálogo no disponible: {e}") from e
        return True

def _exists_sucursal(id_sucursal: int) -> bool:
    if not VALIDATE_SUCURSAL: return True
    candidates = [
        f"{INVENTARIO_BASE_URL}/sucursales/{id_sucursal}",
        f"{INVENTARIO_BASE_URL}/sucursales?id_sucursal={id_sucursal}",
        f"{INVENTARIO_BASE_URL}/sucursales?id={id_sucursal}",
    ]
    for url in candidates:
        try:
            r = http_client.get(url)
            if 200 <= r.status_code < 300:
                # Si retorna JSON, validamos id_sucursal cuando sea posible
                try:
                    data = r.json()
                    if isinstance(data, dict) and "id_sucursal" in data:
                        return int(data["id_sucursal"]) == int(id_sucursal)
                    if isinstance(data, list):
                        return any(int(x.get("id_sucursal",-1)) == int(id_sucursal) for x in data if isinstance(x, dict))
                except Exception:
                    pass
                return True
        except httpx.RequestError:
            continue
    if FAIL_CLOSED:
        raise HTTPException(status_code=502, detail="Inventario no disponible o ruta /sucursales no compatible")
    return True

# =============== Schemas (Swagger) ===============
class RecetaCreate(BaseModel):
    """[Doc] Cuerpo para crear receta."""  # [Doc]
    id_sucursal: int = Field(description="ID de la sucursal (validado contra Inventario)")
    nombre_paciente: Optional[str] = Field(default=None, description="Nombre del paciente (opcional)")

class LineaCreate(BaseModel):
    """[Doc] Línea de receta (upsert por id_producto)."""  # [Doc]
    id_producto: str = Field(min_length=1, description="ID del producto (validado contra Catálogo)")
    cantidad: int = Field(gt=0, description="Cantidad de unidades a recetar")

class DispensacionCreate(BaseModel):
    """[Doc] Registrar una dispensación total para una receta."""  # [Doc]
    id_receta: int = Field(description="ID de la receta")
    cantidad_total: Optional[int] = Field(default=None, ge=0, description="Total dispensado (opcional)")

# =============== Endpoints ===============
@app.get(f"{BASE_PATH}/healthz", tags=["Health"], summary="Health check")  # [Doc]
def healthz():
    return {"status": "ok"}

@app.post(
    "/recetas",
    tags=["Recetas"],                                   # [Doc]
    summary="Crear receta",                             # [Doc]
    description="Crea una receta NUEVA. Valida `id_sucursal` contra Inventario.",  # [Doc]
    status_code=201
)
def crear_receta(body: RecetaCreate):
    if not _exists_sucursal(body.id_sucursal):
        raise HTTPException(400, f"Sucursal {body.id_sucursal} no existe")
    with Session() as s:
        r = Receta(id_sucursal=body.id_sucursal, nombre_paciente=body.nombre_paciente)
        s.add(r); s.commit(); s.refresh(r)
        return {
            "id_receta": r.id_receta, "id_sucursal": r.id_sucursal,
            "nombre_paciente": r.nombre_paciente, "fecha_receta": r.fecha_receta,
            "estado": r.estado
        }

@app.get(
    "/recetas",
    tags=["Recetas"],                                   # [Doc]
    summary="Listar Recetas",                           # [Doc]
    description=(                                       # [Doc]
        "Lista recetas filtrando por `estado` (Enum) y/o rango `[desde, hasta]` **inclusive**.\n"
        "- Acepta ISO-8601: `2025-10-05T20:13:12Z`, `2025-10-05T20:13:12`, `2025-10-05`.\n"
        "- Si envías solo fecha, se asume `00:00:00Z`.\n"
        "- Internamente convertimos a **UTC** y comparamos en UTC."
    )
)
def listar_recetas(
    estado: Optional[EstadoReceta] = Query(  # Enum → despliega menú
        default=None,
        description="Filtra por estado (Enum)"
    ),
    desde: Annotated[
        Optional[datetime | date],
        Query(
            description="Fecha/hora mínima (UTC). Ej: `2025-10-05T20:13:12Z` o `2025-10-05`",
            examples={"iso": {"value": "2025-10-05T20:13:12Z"}, "solo_fecha": {"value": "2025-10-05"}}
        )
    ] = None,
    hasta: Annotated[
        Optional[datetime | date],
        Query(
            description="Fecha/hora máxima (UTC). Inclusiva.",
            examples={"iso": {"value": "2025-10-05T23:59:59Z"}, "igual_que_desde": {"value": "2025-10-05T20:13:12Z"}}
        )
    ] = None
):
    # Normalizamos fechas a UTC naive y soportamos 'date' puro
    d_desde = None
    d_hasta = None
    if isinstance(desde, date) and not isinstance(desde, datetime):
        d_desde = date_to_start_utc(desde)
    elif isinstance(desde, datetime):
        d_desde = to_utc_naive(desde)

    if isinstance(hasta, date) and not isinstance(hasta, datetime):
        d_hasta = date_to_start_utc(hasta)
    elif isinstance(hasta, datetime):
        d_hasta = to_utc_naive(hasta)

    with Session() as s:
        q = s.query(Receta)
        if estado:
            q = q.filter(Receta.estado == estado.value)
        if d_desde:
            q = q.filter(Receta.fecha_receta >= d_desde)
        if d_hasta:
            q = q.filter(Receta.fecha_receta <= d_hasta)
        q = q.order_by(Receta.id_receta.desc())
        return [{
            "id_receta": r.id_receta, "id_sucursal": r.id_sucursal,
            "nombre_paciente": r.nombre_paciente, "fecha_receta": r.fecha_receta,
            "estado": r.estado
        } for r in q.all()]

@app.get(
    "/recetas/{id_receta}",
    tags=["Recetas"], summary="Obtener receta (con detalle)",  # [Doc]
    description="Devuelve encabezado + arreglo `detalle` (líneas)."
)
def obtener_receta(id_receta: int):
    with Session() as s:
        r = s.get(Receta, id_receta)
        if not r: raise HTTPException(404, "No existe")
        det = s.query(RecetaDetalle).filter_by(id_receta=id_receta).all()
        return {
            "id_receta": r.id_receta, "id_sucursal": r.id_sucursal,
            "nombre_paciente": r.nombre_paciente, "fecha_receta": r.fecha_receta,
            "estado": r.estado,
            "detalle": [{"id_producto": d.id_producto, "cantidad": d.cantidad} for d in det]
        }

@app.post(
    "/recetas/{id_receta}/detalle",
    tags=["Recetas"], summary="Agregar/actualizar línea",  # [Doc]
    description=(
        "Upsert por `(id_receta, id_producto)`.\n"
        "- **201** si se creó la línea.\n"
        "- **200** si ya existía y solo se actualizó `cantidad`.\n"
        "- Valida producto contra **Catálogo**."
    )
)
def agregar_linea(id_receta: int, body: LineaCreate):
    if not _exists_producto(body.id_producto):
        raise HTTPException(400, f"Producto {body.id_producto} no existe")
    with Session() as s:
        if not s.get(Receta, id_receta): raise HTTPException(404, "Receta no existe")

        # ¿existe ya la línea?
        existing = s.execute(
            select(RecetaDetalle).where(
                RecetaDetalle.id_receta == id_receta,
                RecetaDetalle.id_producto == body.id_producto
            )
        ).scalar_one_or_none()

        if existing:
            existing.cantidad = body.cantidad
            s.commit()
            return JSONResponse({"ok": True, "message": "Línea actualizada"}, status_code=200)

        d = RecetaDetalle(id_receta=id_receta, id_producto=body.id_producto, cantidad=body.cantidad)
        s.add(d); s.commit()
        return JSONResponse({"ok": True, "message": "Línea creada"}, status_code=201)

@app.post(
    "/dispensaciones",
    tags=["Dispensaciones"], summary="Registrar dispensación",   # [Doc]
    description="Registra una dispensación total para la receta; cambia estado a `DISPENSADA` si aún no lo está."
)
def registrar_dispensacion(body: DispensacionCreate):
    with Session() as s:
        r = s.get(Receta, body.id_receta)
        if not r: raise HTTPException(404, "Receta no existe")
        x = Dispensacion(id_receta=body.id_receta, cantidad_total=body.cantidad_total)
        s.add(x)
        if r.estado != EstadoReceta.DISPENSADA.value:
            r.estado = EstadoReceta.DISPENSADA.value
        s.commit(); s.refresh(x)
        return {
            "id": x.id, "id_receta": x.id_receta,
            "fecha_dispensacion": x.fecha_dispensacion, "cantidad_total": x.cantidad_total
        }
