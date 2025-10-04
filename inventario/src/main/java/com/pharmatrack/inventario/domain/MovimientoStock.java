package com.pharmatrack.inventario.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.OffsetDateTime;

@Entity
@Table(name = "movimiento_stock")
@Data
public class MovimientoStock {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "id_sucursal", nullable = false)
  private Integer id_sucursal;

  @Column(name = "id_producto", nullable = false)
  private String id_producto;

  @Column(name = "tipo_movimiento", nullable = false)
  private String tipo_movimiento; // ENTRADA | EGRESO

  @Column(nullable = false)
  private Integer cantidad;

  @Column(name = "fecha_movimiento", nullable = false)
  private OffsetDateTime fecha_movimiento;

  private String motivo;
}
