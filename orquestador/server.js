import express from "express";
import axios from "axios";
import swaggerUi from "swagger-ui-express";
import YAML from "yamljs";
import cors from "cors";

const app = express();

const origins = (process.env.CORS_ORIGINS || "*")
  .split(",").map(s => s.trim());
app.use(cors({
  origin: origins.includes("*") ? true : origins,
  credentials: true,
  allowedHeaders: ["Content-Type","Authorization"],
  methods: ["GET","POST","PUT","PATCH","DELETE","OPTIONS"]
}));

const http = axios.create({ timeout: Number(process.env.UPSTREAM_TIMEOUT_MS || 5000), maxRedirects: 3 });

const PORT = Number(process.env.PORT || 8085);
const INVENTARIO_URL = process.env.INVENTARIO_URL || "http://inventario:8082";
const RECETAS_URL    = process.env.RECETAS_URL    || "http://recetas:8083";
const CATALOGO_URL   = process.env.CATALOGO_URL   || "http://catalogo:8084";
const BASE_PATH      = (process.env.BASE_PATH || "").replace(/\/+$/, "");

const r = express.Router();

r.get("/disponibilidad", async (req, res) => {
  try {
    const { id_producto, distrito } = req.query;
    if (!id_producto) return res.status(400).json({error:"id_producto requerido"});

    const [prod, stock] = await Promise.all([
      http.get(`${CATALOGO_URL}/productos/${id_producto}`).then(r => r.data),
      http.get(`${INVENTARIO_URL}/stock`, { params: { id_producto, distrito } }).then(r => r.data)
    ]);
    return res.json({ producto: prod, sucursales: stock });
  } catch (e) {
    return res.status(502).json({ error: "Upstream error", detail: String(e) });
  }
});

r.get("/recetas/:id_receta/validacion", async (req, res) => {
  try {
    const receta = await http.get(`${RECETAS_URL}/recetas/${req.params.id_receta}`).then(r => r.data);
    const items = await Promise.all((receta.detalle || []).map(async it => {
      const st = await http.get(`${INVENTARIO_URL}/stock`, { params: { id_producto: it.id_producto } }).then(r => r.data);
      const total = (st || []).reduce((a, s) => a + (s.cantidad_actual ?? s.stock_actual ?? 0), 0);
      const sugerida = (st || []).find(x => (x.cantidad_actual ?? x.stock_actual ?? 0) >= it.cantidad)?.id_sucursal || null;
      return { id_producto: it.id_producto, solicitado: it.cantidad, disponible: total, id_sucursal_sugerida: sugerida };
    }));
    const ok = items.every(i => i.disponible >= i.solicitado);
    return res.json({
      id_receta: receta.id_receta,
      estado_sugerido: ok ? "VALIDADA" : (items.some(i => i.disponible > 0) ? "PARCIAL" : "RECHAZADA"),
      items
    });
  } catch (e) {
    return res.status(502).json({ error: "Upstream error", detail: String(e) });
  }
});

r.get("/healthz", (_req, res) => res.json({ status: "ok" }));

if (process.env.SERVE_DOCS === "1") {
  const spec = YAML.load(process.env.OPENAPI_FILE || "./docs/orquestador.yaml");
  r.use("/swagger", swaggerUi.serve, swaggerUi.setup(spec));
}

app.use(BASE_PATH || "/", r);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`orquestador escuchando en :${PORT} (base='${BASE_PATH || "/"}')`);
});
