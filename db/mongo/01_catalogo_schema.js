db.createCollection('productos', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['_id', 'nombre'],
      additionalProperties: true,
      properties: {
        _id: { bsonType: 'string', description: 'ID público canónico del producto (string)' },
        nombre: { bsonType: 'string', minLength: 1 },
        codigo_atc: { bsonType: ['string', 'null'] },
        requiere_receta: { bsonType: ['bool', 'null'] },
        habilitado: { bsonType: ['bool', 'null'] },
        keywords: { bsonType: ['array', 'null'], items: { bsonType: 'string' } },
        variantes: {
          bsonType: ['array', 'null'],
          items: {
            bsonType: 'object',
            required: ['codigo_barras'],
            properties: {
              codigo_barras: { bsonType: 'string' },
              forma_farmaceutica: { bsonType: ['string', 'null'] },
              concentracion_dosis: { bsonType: ['string', 'null'] },
              unidades_por_paquete: { bsonType: ['int', 'long', 'null'] }
            }
          }
        },
        creado_en: { bsonType: ['date', 'null'] },
        actualizado_en: { bsonType: ['date', 'null'] }
      }
    }
  },
  validationLevel: 'moderate'
});