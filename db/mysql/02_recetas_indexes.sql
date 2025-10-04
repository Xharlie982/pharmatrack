-- Índices "ultra necesarios" para Recetas (MySQL)
CREATE INDEX idx_receta_estado_fecha ON receta(estado, fecha_receta);
CREATE INDEX idx_detalle_id_producto ON receta_detalle(id_producto);
CREATE INDEX idx_disp_receta_fecha   ON dispensacion(id_receta, fecha_dispensacion);
