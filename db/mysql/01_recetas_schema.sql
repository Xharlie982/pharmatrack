-- Base: recetas (MySQL 8.0+)
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE IF NOT EXISTS receta (
  id_receta        BIGINT PRIMARY KEY AUTO_INCREMENT,
  id_sucursal      INT        NOT NULL,          -- FK LÓGICA → Inventario.sucursal
  nombre_paciente  VARCHAR(100),
  fecha_receta     DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  estado           ENUM('NUEVA','VALIDADA','DISPENSADA','ANULADA')
                   NOT NULL DEFAULT 'NUEVA'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS receta_detalle (
  id_receta     BIGINT      NOT NULL,            -- FK REAL → receta
  id_producto   VARCHAR(64) NOT NULL,            -- FK LÓGICA → Catálogo.productos
  cantidad      INT         NOT NULL,
  PRIMARY KEY (id_receta, id_producto),
  CONSTRAINT fk_receta_detalle__receta
    FOREIGN KEY (id_receta) REFERENCES receta(id_receta)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dispensacion (
  id                   BIGINT PRIMARY KEY AUTO_INCREMENT,
  id_receta            BIGINT     NOT NULL,      -- FK REAL → receta
  fecha_dispensacion   DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  cantidad_total       INT,
  CONSTRAINT fk_dispensacion__receta
    FOREIGN KEY (id_receta) REFERENCES receta(id_receta)
    ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
