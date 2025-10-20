import express from "express";
import axios from "axios";
import swaggerUi from "swagger-ui-express";
import YAML from "yamljs";
import cors from "cors";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "1mb" }));

// CORS
const origins = (process.env.CORS_ORIGINS || "*").split(",").map(s => s.trim());
app.use(cors({
  origin: origins.includes("*") ? true : origins,
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
}));

// Config
const PORT = Number(process.env.ORQUESTADOR_PORT || 8085);
const BASE_PATH = (process.env.ORQUESTADOR_BASE_PATH || "/orquestador").replace(/\/+$/, "");

const INVENTARIO_URL = (process.env.INVENTARIO_URL).replace(/\/+$/, "");
const RECETAS_URL    = (process.env.RECETAS_URL).replace(/\/+$/, "");
const CATALOGO_URL   = (process.env.CATALOGO_URL).replace(/\/+$/, "");

const http = axios.create({ timeout: Number(process.env.UPSTREAM_TIMEOUT_MS || 5000) });
const cid = req => req.header("X-Correlation-Id") || crypto.randomUUID();

// Router base
const r = express.Router();
r.get("/", (_req, res) => res.redirect("./docs"));
r.get("/healthz", (_req, res) => res.json({ status: "ok" }));

// -------- Disponibilidad
r.get("/disponibilidad", async (req, res, next) => {
  const c = cid(req);
  try {
    const productoId = req.query.producto;
    const keyword = req.query.keyword;
    const distrito = req.query.distrito;
    const stockMinimo = Number(req.query.stock_minimo ?? 1);

    if (!distrito) {
      return res.status(400).json({ message: "El parámetro 'distrito' es requerido" });
    }
    if (!productoId && !keyword) {
      return res.status(400).json({ message: "Se requiere el parámetro 'producto' o 'keyword'" });
    }

    let productos = [];
    const httpOptions = { headers: { "X-Correlation-Id": c } };

    if (productoId) {
      const p = await http.get(`${CATALOGO_URL}/productos/${encodeURIComponent(productoId)}`, httpOptions).then(r => r.data);
      if (p) productos.push(p);
    } else if (keyword) {
      const resultado = await http.get(`${CATALOGO_URL}/productos`, { ...httpOptions, params: { keyword } }).then(r => r.data);
      productos = resultado?.items || [];
    }
    
    if (!productos.length) return res.json({ items: [] });

    const productosActivos = productos.filter(p => p.activo === true);

    const items = await Promise.all(productosActivos.map(async (prod) => {
      const stock = await http.get(`${INVENTARIO_URL}/stock`, {
        ...httpOptions,
        params: { id_producto: prod._id, distrito }
      }).then(r => r.data || []);
      
      const sucursales = stock.filter(s => s.stock_actual >= stockMinimo);
      return { producto: prod, sucursales };
    }));
    
    res.json({ items: items.filter(item => item.sucursales.length > 0) });
  } catch (e) {
    next(e);
  }
});

// -------- Ficha de producto
r.get("/ficha-producto/:productoId", async (req, res, next) => {
  const c = cid(req);
  try {
    const idp = req.params.productoId;
    const prod = await http.get(`${CATALOGO_URL}/productos/${encodeURIComponent(idp)}`, { headers: { "X-Correlation-Id": c } }).then(r => r.data);
    if (!prod) {
      return res.status(404).json({ message: "Producto no existe" });
    }

    const todoElStock = await http.get(`${INVENTARIO_URL}/stock`, { headers: { "X-Correlation-Id": c }, params: { id_producto: idp } }).then(r => r.data || []);
    const todasSucursales = await http.get(`${INVENTARIO_URL}/sucursales`, { headers: { "X-Correlation-Id": c } }).then(r => r.data || []);
    const mapaSucursales = new Map(todasSucursales.map(s => [s.id_sucursal, s.distrito]));
    
    const porDistrito = {};
    for (const s of todoElStock) {
      const distrito = mapaSucursales.get(s.id_sucursal) || "N/A";
      porDistrito[distrito] = (porDistrito[distrito] || 0) + s.stock_actual;
    }
    res.json({ producto: prod, agregados: { stock_por_distrito: porDistrito } });
  } catch (e) {
    next(e);
  }
});

// -------- Pre-validar receta
r.post("/receta/validar", async (req, res, next) => {
  const c = cid(req);
  try {
    const id = req.body?.id_receta;
    if (!id) {
      return res.status(400).json({ message: "id_receta requerido" });
    }

    const receta = await http.get(`${RECETAS_URL}/${encodeURIComponent(id)}`, { headers: { "X-Correlation-Id": c } }).then(r => r.data);
    
    const items = await Promise.all((receta.detalle || []).map(async it => {
      const st = await http.get(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": c },
        params: { id_producto: it.id_producto, id_sucursal: receta.id_sucursal }
      }).then(r => r.data || []);
      const disp = (st || []).reduce((a, s) => a + s.stock_actual, 0);
      return { id_producto: it.id_producto, solicitado: it.cantidad, disponible: disp, ok: disp >= it.cantidad };
    }));

    const todos = items.every(i => i.ok);
    const alguno = items.some(i => i.disponible > 0);
    let estadoSugerido = "RECHAZADA";
    if (todos) estadoSugerido = "VALIDADA";
    else if (alguno) estadoSugerido = "PARCIAL";
    
    res.json({ id_receta: id, items, estado_sugerido: estadoSugerido });
  } catch (e) {
    next(e);
  }
});

// Docs
if (process.env.SERVE_DOCS === "1") {
  const spec = YAML.load("./docs/orquestador.yaml");
  r.use("/docs", swaggerUi.serve, swaggerUi.setup(spec));
}

// Montaje y Manejo de Errores
app.use(BASE_PATH || "/", r);

r.use((_req, res) => {
  res.status(404).json({ detail: "Not Found in this service" });
});

app.use((err, req, res, next) => {
  console.error(`[orquestador] Error en ${req.method} ${req.originalUrl}`, err.message);
  if (err.response) {
    return res.status(err.response.status).json(err.response.data);
  }
  res.status(500).json({ message: "Error interno en el orquestador" });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[orquestador] escuchando en :${PORT}, base='${BASE_PATH || "/"}'`);
});