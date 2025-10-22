# ==========================================================
# === SCRIPT PARA LIMPIAR Y POBLAR LAS BASES DE DATOS    ===
# ==========================================================

echo "### Iniciando proceso de limpieza y poblado masivo... ###"

# --- 1. PostgreSQL (inventario) ---
echo "--- Procesando PostgreSQL (inventario)... ---"
docker exec -i pg16 psql -U carlosalith -d inventario <<'SQL'
  -- Limpieza de tablas para un inicio limpio
  TRUNCATE TABLE movimiento_stock, stock, sucursal RESTART IDENTITY CASCADE;

  -- Poblado de sucursales (Datos Maestros, ~100 registros)
  INSERT INTO sucursal (id_sucursal, nombre, distrito, direccion)
  SELECT
    i,
    'PharmaTrack Sede ' || i,
    CASE (i % 5)
      WHEN 0 THEN 'LIMA'
      WHEN 1 THEN 'MIRAFLORES'
      WHEN 2 THEN 'SAN ISIDRO'
      WHEN 3 THEN 'SURCO'
      ELSE 'LA MOLINA'
    END,
    'Dirección de Prueba ' || i
  FROM generate_series(1, 100) AS i;

  -- Poblado de movimientos y stock (Datos Transaccionales, 25,000 registros)
  DO $$
  DECLARE
    v_id_sucursal INT;
    v_id_producto TEXT;
    v_cantidad INT;
    v_tipo TEXT;
  BEGIN
    FOR i IN 1..25000 LOOP
      v_id_sucursal := (random() * 99 + 1)::INT;
      v_id_producto := 'PROD-' || (random() * 19999 + 1)::INT;
      v_cantidad := (random() * 49 + 1)::INT;
      
      IF random() > 0.3 THEN
        v_tipo := 'ENTRADA';
      ELSE
        v_tipo := 'EGRESO';
      END IF;

      -- Insertar el movimiento SIEMPRE
      INSERT INTO movimiento_stock (id_sucursal, id_producto, tipo_movimiento, cantidad, motivo)
      VALUES (v_id_sucursal, v_id_producto, v_tipo, v_cantidad, 'Carga masiva');
      
      -- ¡LÓGICA CORREGIDA DEFINITIVA!
      -- Actualizar o crear el stock correspondiente al movimiento
      INSERT INTO stock (id_sucursal, id_producto, stock_actual, umbral_reposicion, fecha_actualizacion)
      VALUES (
          v_id_sucursal,
          v_id_producto,
          -- El stock inicial NUNCA puede ser negativo
          GREATEST(0, CASE WHEN v_tipo = 'ENTRADA' THEN v_cantidad ELSE 0 END),
          10,
          NOW()
      )
      ON CONFLICT (id_sucursal, id_producto) DO UPDATE
      SET
          -- El UPDATE NUNCA debe resultar en negativo
          stock_actual = GREATEST(0, stock.stock_actual + (CASE WHEN v_tipo = 'ENTRADA' THEN v_cantidad ELSE -v_cantidad END)),
          fecha_actualizacion = NOW();
      
    END LOOP;
  END $$;
SQL
echo "--- PostgreSQL finalizado. ---"


# --- 2. MySQL (recetas) ---
echo "--- Procesando MySQL (recetas)... ---"
docker exec -i mysql mysql -ucarlosalith -p111821 recetas <<'SQL'
  -- Limpieza de tablas para un inicio limpio
  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE dispensacion;
  TRUNCATE TABLE receta_detalle;
  TRUNCATE TABLE receta;
  SET FOREIGN_KEY_CHECKS = 1;

  -- Procedimiento para generar 10,000 recetas con 1-3 productos cada una (~20,000 detalles)
  DROP PROCEDURE IF EXISTS PoblarRecetas;
  DELIMITER $$
  CREATE PROCEDURE PoblarRecetas()
  BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE j INT;
    DECLARE num_detalles INT;
    WHILE i <= 10000 DO
      -- Insertar la receta
      INSERT INTO receta (id_sucursal, nombre_paciente, fecha_receta)
      VALUES (FLOOR(RAND() * 100) + 1, CONCAT('Paciente ', i), NOW() - INTERVAL FLOOR(RAND() * 30) DAY);

      -- Insertar detalles de la receta (entre 1 y 3 productos)
      SET num_detalles = FLOOR(RAND() * 3) + 1;
      SET j = 1;
      WHILE j <= num_detalles DO
        INSERT INTO receta_detalle (id_receta, id_producto, cantidad)
        VALUES (i, CONCAT('PROD-', FLOOR(RAND() * 19999) + 1), FLOOR(RAND() * 4) + 1);
        SET j = j + 1;
      END WHILE;

      SET i = i + 1;
    END WHILE;
  END$$
  DELIMITER ;

  -- Ejecutar el procedimiento para poblar los datos
  CALL PoblarRecetas();
SQL
echo "--- MySQL finalizado. ---"


# --- 3. MongoDB (catalogo) ---
echo "--- Procesando MongoDB (catalogo)... ---"
docker exec -i mongo mongosh --quiet "mongodb://carlosalith:111821@localhost:27017/catalogo?authSource=admin" <<'JS'
  // Limpieza de la colección para un inicio limpio
  db.productos.deleteMany({});

  // Poblado de productos (20,000 registros en lotes de 1000)
  const batchSize = 1000;
  const totalDocs = 20000;
  
  print(`Iniciando inserción de ${totalDocs} productos...`);
  for (let i = 0; i < totalDocs; i += batchSize) {
    const batch = [];
    for (let j = 0; j < batchSize; j++) {
      const docNum = i + j + 1;
      const producto = {
        _id: `PROD-${docNum}`,
        nombre: `Producto Farmacéutico ${docNum}`,
        codigo_atc: `ATC-${Math.floor(Math.random() * 500)}`,
        requiere_receta: Math.random() > 0.5,
        activo: true, // Todos los productos se crean como activos
        keywords: [`keyword${docNum}`],
        variantes: [{
          codigo_barras: `775${String(docNum).padStart(10, '0')}`,
          forma_farmaceutica: 'Tableta',
          concentracion_dosis: `${Math.floor(Math.random() * 400) + 100} mg`,
          unidades_por_paquete: Math.random() > 0.5 ? 10 : 20
        }],
        creado_en: new Date(),
        actualizado_en: new Date()
      };
      batch.push(producto);
    }
    db.productos.insertMany(batch);
    print(`Insertado lote ${i / batchSize + 1} de ${totalDocs / batchSize}`);
  }
JS
echo "--- MongoDB finalizado. ---"

echo "### ¡Proceso completado! Tus bases de datos están pobladas. ###"