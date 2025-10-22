BEGIN;

CREATE TABLE IF NOT EXISTS sucursal (
  id_sucursal   INT  PRIMARY KEY,
  nombre        TEXT NOT NULL,
  distrito      TEXT NOT NULL,
  direccion     TEXT
);

CREATE TABLE IF NOT EXISTS stock (
  id_sucursal         INT          NOT NULL REFERENCES sucursal(id_sucursal),
  id_producto         TEXT         NOT NULL,                 -- FK lógica a Catálogo
  stock_actual        INT          NOT NULL CHECK (stock_actual >= 0),
  umbral_reposicion   INT          NOT NULL CHECK (umbral_reposicion >= 0),
  fecha_actualizacion TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (id_sucursal, id_producto)
);

CREATE TABLE IF NOT EXISTS movimiento_stock (
  id               BIGSERIAL   PRIMARY KEY,
  id_sucursal      INT         NOT NULL REFERENCES sucursal(id_sucursal),
  id_producto      TEXT        NOT NULL,                   -- FK lógica a Catálogo
  tipo_movimiento  TEXT        NOT NULL CHECK (tipo_movimiento IN ('ENTRADA','EGRESO')),
  cantidad         INT         NOT NULL CHECK (cantidad > 0),
  fecha_movimiento TIMESTAMPTZ NOT NULL DEFAULT now(),
  motivo           TEXT
);

COMMIT;