import express from "express";
import cors from "cors";
import morgan from "morgan";
import mongoose from "mongoose";
import YAML from "yamljs";
import swaggerUi from "swagger-ui-express";

// =======================
// ====== Configuración ======
// =======================
const PORT = Number(process.env.CATALOGO_PORT || 8084);
const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017/catalogo";
const CORS_ORIGINS = (process.env.CORS_ORIGINS || "*")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);
const SERVE_DOCS = process.env.SERVE_DOCS !== "0";

const RAW_BASE = (process.env.CATALOGO_BASE_PATH || "").trim();
const BASE_PATH = RAW_BASE
  ? (RAW_BASE.startsWith("/") ? RAW_BASE : `/${RAW_BASE}`).replace(/\/+$/, "")
  : "";

// =======================
// ====== App Express y Middlewares ======
// =======================
const app = express();
app.set("trust proxy", true);

app.use(cors({
  origin: (origin, cb) => {
    if (!origin || CORS_ORIGINS.includes("*") || CORS_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error("CORS not allowed"), false);
  },
  credentials: true
}));
app.use(express.json({ limit: "1mb" }));
app.use(morgan("tiny"));

// =======================
// ====== Conexión Mongo ======
// =======================
mongoose.set("strictQuery", true);
mongoose
  .connect(MONGO_URL, { autoIndex: true })
  .then(() => console.log(`[catalogo] Conectado a Mongo`))
  .catch((err) => {
    console.error("[catalogo] Error conectando a Mongo:", err.message);
    process.exit(1);
  });

// =======================
// ====== Modelos (Schemas) de Mongoose ======
// =======================
const VarianteSchema = new mongoose.Schema(
  {
    codigo_barras: { type: String, required: true },
    forma_farmaceutica: { type: String, default: null },
    concentracion_dosis: { type: String, default: null },
    unidades_por_paquete: { type: Number, default: null }
  },
  { _id: false }
);

const ProductoSchema = new mongoose.Schema(
  {
    _id: { type: String, required: true },
    nombre: { type: String, required: true },
    codigo_atc: { type: String, default: null },
    requiere_receta: { type: Boolean, default: null },
    activo: { type: Boolean, default: true },
    keywords: { type: [String], default: [] },
    variantes: { type: [VarianteSchema], default: [] },
    creado_en: { type: Date, default: () => new Date() },
    actualizado_en: { type: Date, default: () => new Date() }
  },
  { collection: "productos", versionKey: false }
);

// --- Índices ---
ProductoSchema.index({ nombre: "text", keywords: "text" });
ProductoSchema.index({ "variantes.codigo_barras": 1 }, { unique: true, sparse: true });
ProductoSchema.index({ activo: 1 }); // Índice para 'activo'

// --- Hooks ---
ProductoSchema.pre("save", function (next) {
  this.actualizado_en = new Date();
  if (!this.creado_en) {
    this.creado_en = new Date();
  }
  next();
});

ProductoSchema.pre("findOneAndUpdate", function (next) {
  this.set({ actualizado_en: new Date() });
  next();
});

const Producto = mongoose.model("Producto", ProductoSchema);

// =======================
// ====== API Router (Endpoints) ======
// =======================
const api = express.Router();

api.get("/", (req, res) => {
  if (SERVE_DOCS && req.headers.accept && req.headers.accept.includes("text/html")) {
    return res.redirect(`${BASE_PATH}/docs`);
  }
  return res.json({
    service: "catalogo",
    message: "API de Catálogo de Productos",
    base_path: BASE_PATH || "/",
    health: `${BASE_PATH || ""}/healthz`,
    docs: SERVE_DOCS ? `${BASE_PATH || ""}/docs` : null,
  });
});

api.get("/healthz", (_req, res) => res.json({ status: "ok" }));

// [GET /productos] - Listar productos
api.get("/productos", async (req, res, next) => {
  try {
    const {
      keyword,
      codigo_atc,
      requiere_receta,
      activo,
      tamano_pagina,
      pagina
    } = req.query;

    const q = {};
    if (keyword) q.$text = { $search: String(keyword) };
    if (codigo_atc) q.codigo_atc = String(codigo_atc);
    if (typeof requiere_receta !== "undefined") q.requiere_receta = String(requiere_receta) === "true";
    if (typeof activo !== "undefined") q.activo = String(activo) === "true";

    const limit = Math.min(Math.max(Number(tamano_pagina || 50), 1), 200);
    const page  = Math.max(Number(pagina || 1), 1);
    const skip  = (page - 1) * limit;

    const [items, total] = await Promise.all([
      Producto.find(q).limit(limit).skip(skip).lean(),
      Producto.countDocuments(q)
    ]);

    res.json({ total, pagina: page, tamano_pagina: limit, items });
  } catch (e) {
    next(e);
  }
});

// [POST /productos] - Crear un producto
api.post("/productos", async (req, res, next) => {
  try {
    const created = await Producto.create(req.body);
    res.status(201).json(created);
  } catch (e) {
    next(e);
  }
});

// [GET /productos/:id] - Obtener un producto
api.get("/productos/:id", async (req, res, next) => {
  try {
    const doc = await Producto.findById(req.params.id).lean();
    if (!doc) return res.status(404).json({ detail: "Producto no encontrado" });
    res.json(doc);
  } catch (e) {
    next(e);
  }
});

// [PUT /productos/:id] - Actualizar/Crear un producto
api.put("/productos/:id", async (req, res, next) => {
  try {
    const doc = await Producto.findOneAndUpdate(
      { _id: req.params.id },
      req.body,
      { new: true, upsert: true } 
    ).lean();
    res.json(doc);
  } catch (e) {
    next(e);
  }
});

// [DELETE /productos/:id] - Eliminar un producto
api.delete("/productos/:id", async (req, res, next) => {
  try {
    const result = await Producto.deleteOne({ _id: req.params.id });
    if (result.deletedCount === 0) {
      return res.status(404).json({ detail: "Producto no encontrado" });
    }
    res.status(204).send();
  } catch (e) {
    next(e);
  }
});

if (SERVE_DOCS) {
  const spec = YAML.load("./docs/catalogo.yaml");
  api.use("/docs", swaggerUi.serve, swaggerUi.setup(spec));
}

api.use((_req, res) => res.status(404).json({ detail: "Not Found in this service" }));

// =======================
// ====== Montaje y Arranque ======
// =======================

app.use(BASE_PATH || "/", api);

// Manejador de errores global
app.use((err, _req, res, _next) => {
  console.error("[catalogo] error:", err.message);
  if (err.name === "MongoServerError" && err.code === 11000) {
    return res.status(409).json({ detail: "Recurso duplicado", dupKey: err.keyValue });
  }
  if (err.name === 'ValidationError') {
    return res.status(400).json({ detail: "Validación fallida", errors: err.errors });
  }
  res.status(500).json({ detail: "Error interno" });
});

// Inicia el servidor
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[catalogo] escuchando en http://0.0.0.0:${PORT}`);
  console.log(`[catalogo] Prefijo de ruta (BASE_PATH): "${BASE_PATH || "/"}"`);
});