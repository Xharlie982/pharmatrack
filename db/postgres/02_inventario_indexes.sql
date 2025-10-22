CREATE INDEX IF NOT EXISTS idx_stock_id_producto  ON stock(id_producto);
CREATE INDEX IF NOT EXISTS idx_stock_id_sucursal  ON stock(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_mov_suc_prod_fecha ON movimiento_stock(id_sucursal, id_producto, fecha_movimiento DESC);