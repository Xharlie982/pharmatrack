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
  allowedHeaders: [
    "Content-Type",
    "Authorization",
    "X-Correlation-Id",
    "Idempotency-Key",
    "X-Idempotencia"
  ],
  methods: ["GET","POST","PUT","PATCH","DELETE","OPTIONS"]
}));

// Config
const PORT = Number(process.env.PORT || 8085);
const BASE_PATH = (process.env.BASE_PATH || "/orquestador").replace(/\/+$/, "");

const INVENTARIO_URL = (process.env.INVENTARIO_URL || "http://inventario:8082/inventario").replace(/\/+$/, "");
const RECETAS_URL    = (process.env.RECETAS_URL    || "http://recetas:8083/recetas").replace(/\/+$/, "");
const CATALOGO_URL   = (process.env.CATALOGO_URL   || "http://catalogo:8084/catalogo").replace(/\/+$/, "");

const http = axios.create({
  timeout: Number(process.env.UPSTREAM_TIMEOUT_MS || 4000),
  maxRedirects: 2
});

const cid = req => req.header("X-Correlation-Id") || crypto.randomUUID();

// Idempotencia RAM
const memIdem = new Map();
const IDEM_TTL_MS = 10 * 60 * 1000;
const idemGet = k => {
  const it = memIdem.get(k);
  if (!it) return null;
  if (Date.now() - it.ts > IDEM_TTL_MS) {
    memIdem.delete(k);
    return null;
  }
  return it.result;
};
const idemSet = (k, v) => memIdem.set(k, { ts: Date.now(), result: v });

// Router base
const r = express.Router();
r.get("/", (_req, res) => res.redirect("./docs"));

r.get("/healthz", (_req, res) => res.json({ status: "ok" }));

r.get("/readyz", async (req, res) => {
  try {
    const h = { headers: { "X-Correlation-Id": cid(req) } };
    await Promise.all([
      http.get(`${INVENTARIO_URL}/healthz`, h),
      http.get(`${RECETAS_URL}/healthz`, h),
      http.get(`${CATALOGO_URL}/healthz`, h),
    ]);
    res.json({ status: "ready" });
  } catch {
    res.status(503).json({
      code: "DOWNSTREAM_UNAVAILABLE",
      message: "Algún servicio no responde"
    });
  }
});

// -------- Disponibilidad
r.get("/disponibilidad", async (req, res) => {
  const c = cid(req);
  try {
    // NUEVOS nombres con alias antiguos
    const producto = req.query.producto || req.query.productoId;
    const busqueda = req.query.busqueda || req.query.q;
    const distrito = req.query.distrito;
    const stock_minimo = Number(req.query.stock_minimo ?? req.query.minStock ?? 1);

    if (!distrito) {
      return res.status(400).json({
        code: "VALIDATION_ERROR",
        message: "distrito requerido"
      });
    }

    // Resolver productos
    let productos = [];
    if (producto) {
      const p = await http.get(
        `${CATALOGO_URL}/productos/${encodeURIComponent(producto)}`,
        { headers: { "X-Correlation-Id": c } }
      ).then(r => r.data);
      productos = p ? [p] : [];
    } else if (busqueda) {
      // si tu catálogo aún no admite ?q, simplemente regresará []
      productos = await http.get(
        `${CATALOGO_URL}/productos`,
        { headers: { "X-Correlation-Id": c }, params: { q: busqueda } }
      ).then(r => r.data || []);
    } else {
      return res.status(400).json({
        code: "VALIDATION_ERROR",
        message: "producto o busqueda requerido"
      });
    }
    if (!productos.length) return res.json({ items: [] });

    // Stock por distrito
    const items = [];
    for (const prod of productos) {
      const idp = prod._id || prod.id || producto;
      const st = await http.get(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": c },
        params: { id_producto: idp, distrito }
      }).then(r => r.data || []);
      const sucursales = (st || []).filter(
        s => (s.stock_actual ?? s.cantidad_actual ?? 0) >= stock_minimo
      );
      items.push({ producto: prod, sucursales });
    }
    res.json({ items });
  } catch (e) {
    res.status(502).json({
      code: "DOWNSTREAM_ERROR",
      message: "Fallo consultando disponibilidad",
      details: String(e)
    });
  }
});

// -------- Ficha de producto
r.get("/ficha-producto/:producto", async (req, res) => {
  const c = cid(req);
  try {
    const idp = req.params.producto;
    const prod = await http.get(
      `${CATALOGO_URL}/productos/${encodeURIComponent(idp)}`,
      { headers: { "X-Correlation-Id": c } }
    ).then(r => r.data);

    if (!prod) {
      return res.status(404).json({ code: "NOT_FOUND", message: "Producto no existe" });
    }

    const st = await http.get(
      `${INVENTARIO_URL}/stock`,
      { headers: { "X-Correlation-Id": c }, params: { id_producto: idp } }
    ).then(r => r.data || []);

    const porDistrito = {};
    for (const s of st) {
      const d = (s.distrito || "N/A");
      const n = (s.stock_actual ?? s.cantidad_actual ?? 0);
      porDistrito[d] = (porDistrito[d] || 0) + n;
    }
    res.json({ producto: prod, agregados: { stock_por_distrito: porDistrito } });
  } catch (e) {
    res.status(502).json({
      code: "DOWNSTREAM_ERROR",
      message: "Fallo consultando ficha",
      details: String(e)
    });
  }
});

// -------- Pre-validar receta
r.post("/receta/validar", async (req, res) => {
  const c = cid(req);
  try {
    const id = req.body?.id_receta;
    if (!id) {
      return res.status(400).json({
        code: "VALIDATION_ERROR",
        message: "id_receta requerido"
      });
    }

    // FIX: no duplicar /recetas
    const receta = await http.get(
      `${RECETAS_URL}/${encodeURIComponent(id)}`,
      { headers: { "X-Correlation-Id": c } }
    ).then(r => r.data);

    const items = await Promise.all((receta.detalle || []).map(async it => {
      const st = await http.get(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": c },
        params: { id_producto: it.id_producto, id_sucursal: receta.id_sucursal }
      }).then(r => r.data || []);
      const disp = (st || []).reduce((a, s) => a + (s.stock_actual ?? s.cantidad_actual ?? 0), 0);
      return { id_producto: it.id_producto, solicitado: it.cantidad, disponible: disp, ok: disp >= it.cantidad };
    }));
    const todos = items.every(i => i.ok);
    res.status(todos ? 200 : 207).json({
      id_receta: id,
      items,
      estado_sugerido: todos ? "VALIDADA" : (items.some(i => i.disponible > 0) ? "PARCIAL" : "RECHAZADA")
    });
  } catch (e) {
    res.status(502).json({
      code: "DOWNSTREAM_ERROR",
      message: "Fallo validando receta",
      details: String(e)
    });
  }
});

// -------- Validación por GET (solo lectura)
r.get("/receta/:id/validacion", async (req, res) => {
  const c = cid(req);
  try {
    // FIX: no duplicar /recetas
    const receta = await http.get(
      `${RECETAS_URL}/${encodeURIComponent(req.params.id)}`,
      { headers: { "X-Correlation-Id": c } }
    ).then(r => r.data);

    const items = await Promise.all((receta.detalle || []).map(async it => {
      const st = await http.get(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": c },
        params: { id_producto: it.id_producto }
      }).then(r => r.data || []);
      const total = (st || []).reduce((a, s) => a + (s.stock_actual ?? s.cantidad_actual ?? 0), 0);
      const sugerida = (st || []).find(
        x => (x.stock_actual ?? x.cantidad_actual ?? 0) >= it.cantidad
      )?.id_sucursal || null;
      return {
        id_producto: it.id_producto,
        solicitado: it.cantidad,
        disponible: total,
        id_sucursal_sugerida: sugerida
      };
    }));
    const ok = items.every(i => i.disponible >= i.solicitado);
    res.json({
      id_receta: receta.id_receta,
      estado_sugerido: ok ? "VALIDADA" : (items.some(i => i.disponible > 0) ? "PARCIAL" : "RECHAZADA"),
      items
    });
  } catch (e) {
    res.status(502).json({
      code: "DOWNSTREAM_ERROR",
      message: "Fallo consultando receta/stock",
      details: String(e)
    });
  }
});

// -------- Reserva efímera
const memReservas = new Map(); // id_receta -> { vence, items }
const RESERVA_TTL_MS = 2 * 60 * 1000;

r.post("/reserva-stock", async (req, res) => {
  const c = cid(req);
  // Aceptar ambos encabezados
  const idem = req.header("Idempotency-Key")
            || req.header("X-Idempotencia")
            || req.body?.idempotencia_clave
            || req.body?.idempotency_key;
  try {
    const id = req.body?.id_receta;
    if (!id) {
      return res.status(400).json({
        code: "VALIDATION_ERROR",
        message: "id_receta requerido"
      });
    }
    if (idem) {
      const cache = idemGet(idem);
      if (cache) return res.json(cache);
    }

    // FIX: no duplicar /recetas
    const receta = await http.get(
      `${RECETAS_URL}/${encodeURIComponent(id)}`,
      { headers: { "X-Correlation-Id": c } }
    ).then(r => r.data);

    const faltantes = [];
    for (const it of (receta.detalle || [])) {
      const st = await http.get(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": c },
        params: { id_producto: it.id_producto, id_sucursal: receta.id_sucursal }
      }).then(r => r.data || []);
      const disp = (st || []).reduce((a, s) => a + (s.stock_actual ?? s.cantidad_actual ?? 0), 0);
      if (disp < it.cantidad) {
        faltantes.push({ id_producto: it.id_producto, requerido: it.cantidad, disponible: disp });
      }
    }
    if (faltantes.length) {
      return res.status(409).json({
        code: "STOCK_INSUFICIENTE",
        message: "No alcanza para reservar",
        details: faltantes
      });
    }

    memReservas.set(String(id), { vence: Date.now() + RESERVA_TTL_MS, items: receta.detalle });
    const result = { ok: true, id_receta: id, vence_en_ms: RESERVA_TTL_MS };
    if (idem) idemSet(idem, result);
    res.json(result);
  } catch (e) {
    res.status(502).json({
      code: "DOWNSTREAM_ERROR",
      message: "Fallo al reservar",
      details: String(e)
    });
  }
});

// Docs
if (process.env.SERVE_DOCS === "1") {
  const spec = YAML.load(process.env.OPENAPI_FILE || "./docs/orquestador.yaml");
  r.use("/docs", swaggerUi.serve, swaggerUi.setup(spec));
}

app.use(BASE_PATH || "/", r);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`orquestador :${PORT} base='${BASE_PATH || "/"}'`);
});
