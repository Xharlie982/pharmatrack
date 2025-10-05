import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from sqlalchemy import create_engine, Integer, String, DateTime, Enum, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Mapped, mapped_column

# --- HTTP client para validaciones externas ---
import httpx

def _as_bool(v: Optional[str], default: bool = True) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y")

# ===== Config =====
DB_URL = os.getenv("MYSQL_URL", "mysql+pymysql://user:pass@127.0.0.1:3306/recetas")
BASE_PATH = (os.getenv("BASE_PATH", "")).rstrip("/")  # ej: "/recetas"

CATALOGO_BASE_URL = os.getenv("CATALOGO_BASE_URL", "http://localhost:8084/catalogo").rstrip("/")
INVENTARIO_BASE_URL = os.getenv("INVENTARIO_BASE_URL", "http://localhost:8082/inventario").rstrip("/")

VALIDATE_PRODUCTO = _as_bool(os.getenv("VALIDATE_PRODUCTO", "1"), True)
VALIDATE_SUCURSAL = _as_bool(os.getenv("VALIDATE_SUCURSAL", "1"), True)
FAIL_CLOSED = _as_bool(os.getenv("FAIL_CLOSED", "1"), True)  # error llamando a otros svcs => 502

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "3.0"))

http_client = httpx.Client(timeout=HTTP_TIMEOUT)

# ===== DB =====
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
Base = declarative_base()

# ===== Modelos =====
class Receta(Base):
    __tablename__ = "receta"
    id_receta: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_sucursal: Mapped[int] = mapped_column(Integer, nullable=False)
    nombre_paciente: Mapped[Optional[str]] = mapped_column(String(100))
    fecha_receta: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    estado: Mapped[str] = mapped_column(
        Enum('NUEVA','VALIDADA','DISPENSADA','ANULADA', name="estado_receta"),
        default='NUEVA', nullable=False
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
    fecha_dispensacion: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    cantidad_total: Mapped[Optional[int]] = mapped_column(Integer)

# No alterará tu esquema si ya existe
Base.metadata.create_all(engine)

# ===== FastAPI + CORS =====
app = FastAPI(title="Recetas & Dispensación API", version="1.0.1")

cors_origins = os.getenv("CORS_ORIGINS", "*")
origins = [o.strip() for o in cors_origins.split(",")] if cors_origins else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Cerrar httpx al apagar
@app.on_event("shutdown")
def _shutdown():
    try:
        http_client.close()
    except Exception:
        pass

# Middleware para “strip” del prefijo cuando el LB no reescribe
from starlette.middleware.base import BaseHTTPMiddleware
class StripPrefixMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, prefix: str):
        super().__init__(app); self.prefix = prefix
    async def dispatch(self, request, call_next):
        path = request.scope.get("path","")
        if self.prefix and path.startswith(self.prefix):
            request.scope["path"] = path[len(self.prefix):] or "/"
        elif self.prefix:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail":"Not found"}, status_code=404)
        return await call_next(request)

if BASE_PATH:
    app.add_middleware(StripPrefixMiddleware, prefix=BASE_PATH)

# ===== Helpers: validaciones externas =====
def _exists_producto(id_producto: str) -> bool:
    if not VALIDATE_PRODUCTO:
        return True
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
    if not VALIDATE_SUCURSAL:
        return True
    # Ajusta este path si tu Inventario expone otro para obtener sucursal por ID
    url = f"{INVENTARIO_BASE_URL}/sucursales/{id_sucursal}"
    try:
        r = http_client.get(url)
        if r.status_code == 404: return False
        return 200 <= r.status_code < 300
    except httpx.RequestError as e:
        if FAIL_CLOSED:
            raise HTTPException(status_code=502, detail=f"Inventario no disponible: {e}") from e
        return True

# ===== Schemas =====
class RecetaCreate(BaseModel):
    id_sucursal: int
    nombre_paciente: Optional[str] = None

class LineaCreate(BaseModel):
    id_producto: str = Field(min_length=1)
    cantidad: int = Field(gt=0)

class DispensacionCreate(BaseModel):
    id_receta: int
    cantidad_total: Optional[int] = Field(default=None, ge=0)

# ===== Endpoints =====
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/recetas", status_code=201)
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

@app.get("/recetas")
def listar_recetas(
    estado: Optional[str] = Query(default=None, pattern="^(NUEVA|VALIDADA|DISPENSADA|ANULADA)$"),
    desde: Optional[datetime] = None,
    hasta: Optional[datetime] = None
):
    with Session() as s:
        q = s.query(Receta)
        if estado: q = q.filter(Receta.estado == estado)
        if desde:  q = q.filter(Receta.fecha_receta >= desde)
        if hasta:  q = q.filter(Receta.fecha_receta <= hasta)
        return [{
            "id_receta": r.id_receta, "id_sucursal": r.id_sucursal,
            "nombre_paciente": r.nombre_paciente, "fecha_receta": r.fecha_receta,
            "estado": r.estado
        } for r in q.all()]

@app.get("/recetas/{id_receta}")
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

@app.post("/recetas/{id_receta}/detalle", status_code=201)
def agregar_linea(id_receta: int, body: LineaCreate):
    if not _exists_producto(body.id_producto):
        raise HTTPException(400, f"Producto {body.id_producto} no existe")
    with Session() as s:
        if not s.get(Receta, id_receta): raise HTTPException(404, "Receta no existe")
        d = RecetaDetalle(id_receta=id_receta, id_producto=body.id_producto, cantidad=body.cantidad)
        s.merge(d); s.commit()
        return {"ok": True, "message": "Línea agregada/actualizada"}

@app.post("/dispensaciones", status_code=201)
def registrar_dispensacion(body: DispensacionCreate):
    with Session() as s:
        r = s.get(Receta, body.id_receta)
        if not r: raise HTTPException(404, "Receta no existe")
        x = Dispensacion(id_receta=body.id_receta, cantidad_total=body.cantidad_total)
        s.add(x)
        if r.estado != "DISPENSADA":
            r.estado = "DISPENSADA"
        s.commit(); s.refresh(x)
        return {
            "id": x.id, "id_receta": x.id_receta,
            "fecha_dispensacion": x.fecha_dispensacion, "cantidad_total": x.cantidad_total
        }

# ===== Swagger embebido (docs/recetas.yaml) =====
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
if os.getenv("SERVE_DOCS", "1") == "1":
    app.mount("/docs", StaticFiles(directory="docs"), name="docs")
    SWAGGER_HTML = """
    <!DOCTYPE html><html><head><meta charset="utf-8"><title>Swagger</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
    </head><body><div id="swagger"></div>
    <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
    <script>window.ui=SwaggerUIBundle({url:'/docs/recetas.yaml',dom_id:'#swagger'});</script>
    </body></html>
    """
    @app.get("/swagger-ui", response_class=HTMLResponse)
    def swagger_ui():
        return SWAGGER_HTML
