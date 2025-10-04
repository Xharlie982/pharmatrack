package com.pharmatrack.inventario.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.OffsetDateTime;

@Entity
@Table(name = "stock")
@IdClass(StockPK.class)
@Data
public class Stock {
  @Id
  @Column(name = "id_sucursal")
  private Integer id_sucursal;

  @Id
  @Column(name = "id_producto")
  private String id_producto;

  @Column(name = "stock_actual", nullable = false)
  private Integer stock_actual;

  @Column(name = "umbral_reposicion", nullable = false)
  private Integer umbral_reposicion;

  @Column(name = "fecha_actualizacion", nullable = false)
  private OffsetDateTime fecha_actualizacion;
}
