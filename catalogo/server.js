import express from "express";
import cors from "cors";
import morgan from "morgan";
import mongoose from "mongoose";
import YAML from "yamljs";
import swaggerUi from "swagger-ui-express";

// --------- Config ----------
const PORT = Number(process.env.PORT || 8084);
const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017/catalogo";
const CORS_ORIGINS = (process.env.CORS_ORIGINS || "*")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);
const SERVE_DOCS = process.env.SERVE_DOCS === "1";

// Prefijo para funcionar detrás del ALB (ej: "/catalogo")
const RAW_BASE = (process.env.BASE_PATH || "").trim();
// normaliza: vacío => "", con barra inicial y sin barra final
const BASE_PATH = RAW_BASE
  ? (RAW_BASE.startsWith("/") ? RAW_BASE : `/${RAW_BASE}`).replace(/\/+$/, "")
  : "";

// --------- App base ----------
const app = express();
app.set("trust proxy", true);

// CORS
app.use(
  cors({
    origin: (origin, cb) => {
      if (!origin || CORS_ORIGINS.includes("*") || CORS_ORIGINS.includes(origin)) return cb(null, true);
      return cb(new Error("CORS not allowed"), false);
    },
    credentials: true
  })
);
app.use(express.json({ limit: "1mb" }));
app.use(morgan("tiny"));

// --------- Mongo ----------
mongoose.set("strictQuery", true);
mongoose
  .connect(MONGO_URL, { autoIndex: true })
  .then(() => console.log("[catalogo] Conectado a Mongo"))
  .catch((err) => {
    console.error("[catalogo] Error conectando a Mongo:", err.message);
    process.exit(1);
  });

// --------- Router con prefijo ----------
const api = express.Router();

// Landing DENTRO del prefijo (p. ej. /catalogo/)
api.get("/", (req, res) => {
  if (SERVE_DOCS && req.headers.accept && req.headers.accept.includes("text/html")) {
    return res.redirect(`${BASE_PATH}/docs`);
  }
  return res.json({
    service: "catalogo",
    base_path: BASE_PATH || "/",
    health: `${BASE_PATH || ""}/healthz`,
    docs: SERVE_DOCS ? `${BASE_PATH || ""}/docs` : null,
    message: "Bienvenido al microservicio Catálogo"
  });
});

// Health
api.get("/healthz", (_req, res) => res.json({ status: "ok" }));

// --------- Modelos ----------
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
    habilitado: { type: Boolean, default: true },
    keywords: { type: [String], default: [] },
    variantes: { type: [VarianteSchema], default: [] },
    creado_en: { type: Date, default: () => new Date() },
    actualizado_en: { type: Date, default: () => new Date() }
  },
  { collection: "productos", versionKey: false }
);

// Índices
ProductoSchema.index({ nombre: "text", keywords: "text" });
ProductoSchema.index({ codigo_atc: 1 });
ProductoSchema.index({ requiere_receta: 1 });
ProductoSchema.index({ habilitado: 1 });
ProductoSchema.index({ "variantes.codigo_barras": 1 }, { unique: true, sparse: true });

ProductoSchema.pre("save", function (next) {
  this.actualizado_en = new Date();
  next();
});
ProductoSchema.pre("findOneAndUpdate", function (next) {
  this.set({ actualizado_en: new Date() });
  next();
});

const Producto = mongoose.model("Producto", ProductoSchema);

// --------- Rutas ----------
api.get("/productos", async (req, res, next) => {
  try {
    // Nuevos nombres claros
    const {
      busqueda,
      codigo_atc,
      requiere_receta,
      habilitado,
      tamano_pagina,
      pagina
    } = req.query;

    // Compatibilidad hacia atrás (si alguien sigue llamando con los viejos)
    const legacy = req.query;
    const _busqueda        = busqueda ?? legacy.texto ?? null;
    const _codigo_atc      = codigo_atc ?? legacy.atc ?? null;
    const _requiere_receta = (typeof requiere_receta !== "undefined")
                              ? requiere_receta
                              : legacy.rx;
    const _habilitado      = (typeof habilitado !== "undefined")
                              ? habilitado
                              : legacy.habilitado;
    const _tamano_pagina   = Number(tamano_pagina ?? legacy.limit ?? 50);
    const _pagina          = Number(pagina ?? (legacy.skip ? (Number(legacy.skip) / _tamano_pagina) + 1 : 1));

    const q = {};
    if (_busqueda) q.$text = { $search: String(_busqueda) };
    if (_codigo_atc) q.codigo_atc = String(_codigo_atc);
    if (typeof _requiere_receta !== "undefined") q.requiere_receta = String(_requiere_receta) === "true";
    if (typeof _habilitado !== "undefined") q.habilitado = String(_habilitado) === "true";

    // paginación: pagina (1-based) -> skip
    const limit = Math.min(Math.max(_tamano_pagina, 1), 200);
    const page  = Math.max(_pagina, 1);
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

api.post("/productos", async (req, res, next) => {
  try {
    const created = await Producto.create(req.body);
    res.status(201).json(created);
  } catch (e) {
    next(e);
  }
});

api.get("/productos/:id", async (req, res, next) => {
  try {
    const doc = await Producto.findById(req.params.id).lean();
    if (!doc) return res.status(404).json({ detail: "No existe" });
    res.json(doc);
  } catch (e) {
    next(e);
  }
});

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

// Path con nombre claro y compatibilidad con /ean
api.get("/productos/codigos-barras/:codigo_barras", async (req, res, next) => {
  try {
    const codigo = req.params.codigo_barras ?? req.params.ean;
    const doc = await Producto.findOne({ "variantes.codigo_barras": codigo }).lean();
    if (!doc) return res.status(404).json({ detail: "No existe" });
    res.json(doc);
  } catch (e) {
    next(e);
  }
});

// Swagger opcional dentro del prefijo (queda en /<base>/docs)
if (SERVE_DOCS) {
  const spec = YAML.load("./docs/catalogo.yaml");
  api.use("/docs", swaggerUi.serve, swaggerUi.setup(spec));
}

// 404 del subrouter
api.use((_req, res) => res.status(404).json({ detail: "Not found" }));

// Monta el subrouter en el BASE_PATH (o raíz si no hay)
app.use(BASE_PATH || "/", api);

// Landing FUERA del prefijo (IP:PUERTO/) para pruebas sin ALB
app.get("/", (req, res) => {
  const info = {
    service: "catalogo",
    base_path: BASE_PATH || "/",
    health: `${BASE_PATH || ""}/healthz`,
    docs: SERVE_DOCS ? `${BASE_PATH || ""}/docs` : null,
    note: "Este endpoint existe solo para pruebas directas por IP/puerto."
  };
  // Si el cliente quiere HTML, redirige a docs
  if (SERVE_DOCS && req.headers.accept && req.headers.accept.includes("text/html")) {
    return res.redirect(info.docs || info.health);
  }
  return res.json(info);
});

// Manejo de errores
app.use((err, _req, res, _next) => {
  console.error("[catalogo] error:", err.message);
  if (err.name === "MongoServerError" && err.code === 11000) {
    return res.status(409).json({ detail: "Duplicado", dupKey: err.keyValue });
  }
  res.status(500).json({ detail: "Error interno" });
});

// --------- Server ----------
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[catalogo] escuchando en :${PORT} (BASE_PATH="${BASE_PATH || "/"}")`);
});
