db.productos.createIndex({ nombre: "text", keywords: "text" });
db.productos.createIndex({ codigo_atc: 1 });
db.productos.createIndex({ requiere_receta: 1 });
db.productos.createIndex({ habilitado: 1 });
db.productos.createIndex({ "variantes.codigo_barras": 1 }, { unique: true, sparse: true });