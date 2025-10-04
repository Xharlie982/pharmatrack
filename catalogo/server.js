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

// --------- App base ----------
const app = express();

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
  .connect(MONGO_URL, {
    autoIndex: true
  })
  .then(() => console.log(`[catalogo] Conectado a Mongo`))
  .catch((err) => {
    console.error("[catalogo] Error conectando a Mongo:", err.message);
    process.exit(1);
  });

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
    _id: { type: String, required: true }, // ID canónico (string)
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

// Índices “ultra necesarios”
ProductoSchema.index({ nombre: "text", keywords: "text" });
ProductoSchema.index({ codigo_atc: 1 });
ProductoSchema.index({ requiere_receta: 1 });
ProductoSchema.index({ habilitado: 1 });
ProductoSchema.index({ "variantes.codigo_barras": 1 }, { unique: true, sparse: true });

// Mantén actualizado `actualizado_en`
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
app.get("/healthz", (_req, res) => res.json({ status: "ok" }));

// Buscar con filtros
app.get("/productos", async (req, res, next) => {
  try {
    const { texto, atc, rx, habilitado, limit = 50, skip = 0 } = req.query;

    const q = {};
    if (texto) q.$text = { $search: texto };
    if (atc) q.codigo_atc = atc;
    if (typeof rx !== "undefined") q.requiere_receta = rx === "true";
    if (typeof habilitado !== "undefined") q.habilitado = habilitado === "true";

    const docs = await Producto.find(q).limit(Number(limit)).skip(Number(skip)).lean();
    res.json(docs);
  } catch (e) {
    next(e);
  }
});

// Crear
app.post("/productos", async (req, res, next) => {
  try {
    const created = await Producto.create(req.body);
    res.status(201).json(created);
  } catch (e) {
    next(e);
  }
});

// Obtener por ID canónico
app.get("/productos/:id", async (req, res, next) => {
  try {
    const doc = await Producto.findById(req.params.id).lean();
    if (!doc) return res.status(404).json({ detail: "No existe" });
    res.json(doc);
  } catch (e) {
    next(e);
  }
});

// Upsert por ID
app.put("/productos/:id", async (req, res, next) => {
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

// Buscar por código de barras (en variantes)
app.get("/productos/codigos-barras/:ean", async (req, res, next) => {
  try {
    const doc = await Producto.findOne({ "variantes.codigo_barras": req.params.ean }).lean();
    if (!doc) return res.status(404).json({ detail: "No existe" });
    res.json(doc);
  } catch (e) {
    next(e);
  }
});

// Swagger opcional
if (SERVE_DOCS) {
  const spec = YAML.load("./docs/catalogo.yaml");
  app.use("/docs", swaggerUi.serve, swaggerUi.setup(spec));
}

// 404
app.use((_req, res) => res.status(404).json({ detail: "Not found" }));

// Manejo de errores
// (devuelve JSON y evita trazas crudas en producción)
app.use((err, _req, res, _next) => {
  console.error("[catalogo] error:", err.message);
  if (err.name === "MongoServerError" && err.code === 11000) {
    return res.status(409).json({ detail: "Duplicado", dupKey: err.keyValue });
  }
  res.status(500).json({ detail: "Error interno" });
});

// --------- Server ----------
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[catalogo] escuchando en :${PORT}`);
});
