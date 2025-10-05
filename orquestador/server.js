import express from "express";
import axios from "axios";
import swaggerUi from "swagger-ui-express";
import YAML from "yamljs";
import cors from "cors";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "1mb" }));

// --- CORS ---
const origins = (process.env.CORS_ORIGINS || "*").split(",").map(s => s.trim());
app.use(cors({
  origin: origins.includes("*") ? true : origins,
  credentials: true,
  allowedHeaders: ["Content-Type","Authorization","X-Correlation-Id","Idempotency-Key"],
  methods: ["GET","POST","PUT","PATCH","DELETE","OPTIONS"]
}));

// --- Config ---
const PORT = Number(process.env.PORT || 8085);
const BASE_PATH = (process.env.BASE_PATH || "/orquestador").replace(/\/+$/, "");

const INVENTARIO_URL = (process.env.INVENTARIO_URL || "http://inventario:8082/inventario").replace(/\/+$/, "");
const RECETAS_URL    = (process.env.RECETAS_URL    || "http://recetas:8083/recetas").replace(/\/+$/, "");
const CATALOGO_URL   = (process.env.CATALOGO_URL   || "http://catalogo:8084/catalogo").replace(/\/+$/, "");

// --- HTTP client (timeouts y mínimo reintento en lecturas) ---
const http = axios.create({
  timeout: Number(process.env.UPSTREAM_TIMEOUT_MS || 4000),
  maxRedirects: 2,
});
async function safeGet(url, config = {}, retries = 1) {
  try { return await http.get(url, config); }
  catch (e) {
    if (retries > 0) return await safeGet(url, config, retries - 1);
    throw e;
  }
}

// --- Util: correlación e idempotencia in-memory ---
const memIdem = new Map(); // key -> {ts, result}
const IDEM_TTL_MS = 10 * 60 * 1000;

function getCorrelationId(req) {
  return req.header("X-Correlation-Id") || crypto.randomUUID();
}
function idemGet(key) {
  const it = memIdem.get(key);
  if (!it) return null;
  if (Date.now() - it.ts > IDEM_TTL_MS) { memIdem.delete(key); return null; }
  return it.result;
}
function idemSet(key, result) { memIdem.set(key, { ts: Date.now(), result }); }

// --- Router ---
const r = express.Router();

// healthz: vivo
r.get("/healthz", (_req, res) => res.json({ status: "ok" }));

// readyz: pings rápidos a downstreams
r.get("/readyz", async (req, res) => {
  try {
    const hdrs = { headers: { "X-Correlation-Id": getCorrelationId(req) } };
    await Promise.all([
      safeGet(`${INVENTARIO_URL}/healthz`, hdrs),
      safeGet(`${RECETAS_URL}/healthz`, hdrs),
      safeGet(`${CATALOGO_URL}/healthz`, hdrs),
    ]);
    res.json({ status: "ready" });
  } catch {
    res.status(503).json({ code:"DOWNSTREAM_UNAVAILABLE", message:"Algún servicio no responde" });
  }
});

// GET /disponibilidad
r.get("/disponibilidad", async (req, res) => {
  const cid = getCorrelationId(req);
  try {
    const { id_producto, distrito } = req.query;
    if (!id_producto) return res.status(400).json({ code:"VALIDATION_ERROR", message:"id_producto requerido", correlation_id: cid });

    const [prod, stock] = await Promise.all([
      safeGet(`${CATALOGO_URL}/productos/${encodeURIComponent(id_producto)}`,
              { headers: { "X-Correlation-Id": cid } }).then(r => r.data),
      safeGet(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": cid },
        params: { id_producto, distrito }
      }).then(r => r.data)
    ]);

    return res.json({ producto: prod, sucursales: stock });
  } catch (e) {
    return res.status(502).json({ code:"DOWNSTREAM_ERROR", message:"Fallo consultando catálogo/inventario", details:String(e), correlation_id: cid });
  }
});

// GET /recetas/:id_receta/validacion
r.get("/recetas/:id_receta/validacion", async (req, res) => {
  const cid = getCorrelationId(req);
  try {
    const receta = await safeGet(`${RECETAS_URL}/recetas/${encodeURIComponent(req.params.id_receta)}`,
                                 { headers: { "X-Correlation-Id": cid } }).then(r => r.data);
    const items = await Promise.all((receta.detalle || []).map(async it => {
      const st = await safeGet(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": cid },
        params: { id_producto: it.id_producto }
      }).then(r => r.data);
      const total = (st || []).reduce((a, s) => a + (s.stock_actual ?? s.cantidad_actual ?? 0), 0);
      const sugerida = (st || []).find(x => (x.stock_actual ?? x.cantidad_actual ?? 0) >= it.cantidad)?.id_sucursal || null;
      return { id_producto: it.id_producto, solicitado: it.cantidad, disponible: total, id_sucursal_sugerida: sugerida };
    }));
    const ok = items.every(i => i.disponible >= i.solicitado);
    return res.json({
      id_receta: receta.id_receta,
      estado_sugerido: ok ? "VALIDADA" : (items.some(i => i.disponible > 0) ? "PARCIAL" : "RECHAZADA"),
      items
    });
  } catch (e) {
    return res.status(502).json({ code:"DOWNSTREAM_ERROR", message:"Fallo consultando receta/stock", details:String(e), correlation_id: cid });
  }
});

// POST /dispensar  { id_receta, idempotency_key? }
r.post("/dispensar", async (req, res) => {
  const cid = getCorrelationId(req);
  const idemKey = req.header("Idempotency-Key") || req.body?.idempotency_key;
  try {
    if (!req.body?.id_receta) return res.status(422).json({ code:"VALIDATION_ERROR", message:"id_receta requerido", correlation_id: cid });
    if (idemKey) {
      const cached = idemGet(idemKey);
      if (cached) return res.json(cached);
    }

    // 1) Traer receta
    const receta = await safeGet(`${RECETAS_URL}/recetas/${encodeURIComponent(req.body.id_receta)}`,
                                 { headers: { "X-Correlation-Id": cid } }).then(r => r.data);
    const lineas = receta?.detalle || [];
    if (!lineas.length) return res.status(422).json({ code:"VALIDATION_ERROR", message:"Receta sin líneas", correlation_id: cid });

    // 2) Verificar stock por línea
    const faltantes = [];
    const porEgresar = [];
    for (const it of lineas) {
      const st = await safeGet(`${INVENTARIO_URL}/stock`, {
        headers: { "X-Correlation-Id": cid },
        params: { id_producto: it.id_producto, id_sucursal: receta.id_sucursal } // intenta en sucursal de la receta
      }).then(r => r.data);

      const disponibleSuc = (st || []).reduce((a, s) => a + (s.stock_actual ?? s.cantidad_actual ?? 0), 0);
      if (disponibleSuc < it.cantidad) {
        faltantes.push({ id_producto: it.id_producto, requerido: it.cantidad, disponible: disponibleSuc });
      } else {
        porEgresar.push({ id_sucursal: receta.id_sucursal, id_producto: it.id_producto, cantidad: it.cantidad });
      }
    }
    if (faltantes.length) {
      return res.status(409).json({ code:"STOCK_INSUFICIENTE", message:"No hay stock suficiente en la sucursal de la receta", details: faltantes, correlation_id: cid });
    }

    // 3) Egresos (aplica movimientos)
    const movimientosHechos = [];
    try {
      for (const m of porEgresar) {
        const body = { id_sucursal: m.id_sucursal, id_producto: m.id_producto, tipo_movimiento: "EGRESO", cantidad: m.cantidad, motivo: `dispensacion:${receta.id_receta}` };
        const resp = await http.post(`${INVENTARIO_URL}/movimientos`, body, { headers: { "X-Correlation-Id": cid } });
        movimientosHechos.push(resp.data);
      }
    } catch (egresoErr) {
      // 3b) Compensación: revertir lo aplicado
      for (const m of movimientosHechos) {
        try {
          await http.post(`${INVENTARIO_URL}/movimientos`, {
            id_sucursal: m.id_sucursal, id_producto: m.id_producto, tipo_movimiento: "ENTRADA", cantidad: m.cantidad, motivo: `rollback:${receta.id_receta}`
          }, { headers: { "X-Correlation-Id": cid } });
        } catch {}
      }
      throw egresoErr;
    }

    // 4) Crear dispensación
    const disp = await http.post(`${RECETAS_URL}/dispensaciones`, {
      id_receta: receta.id_receta,
      cantidad_total: porEgresar.reduce((a, x) => a + x.cantidad, 0)
    }, { headers: { "X-Correlation-Id": cid } }).then(r => r.data);

    const result = {
      ok: true,
      id_receta: receta.id_receta,
      estado_final: "DISPENSADA",
      movimientos: porEgresar.map(m => ({ ...m, tipo: "EGRESO" })),
      correlation_id: cid,
      message: "Dispensación completada",
      details: { dispensacion: disp }
    };
    if (idemKey) idemSet(idemKey, result);
    return res.json(result);

  } catch (e) {
    return res.status(502).json({ code:"DOWNSTREAM_ERROR", message:"Fallo coordinando dispensación", details:String(e), correlation_id: cid });
  }
});

// --- Swagger ---
if (process.env.SERVE_DOCS === "1") {
  const spec = YAML.load(process.env.OPENAPI_FILE || "./docs/orquestador.yaml");
  r.use("/swagger", swaggerUi.serve, swaggerUi.setup(spec));
}

// Montaje con base path del ALB
app.use(BASE_PATH || "/", r);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`orquestador en :${PORT} base='${BASE_PATH || "/"}'`);
});
