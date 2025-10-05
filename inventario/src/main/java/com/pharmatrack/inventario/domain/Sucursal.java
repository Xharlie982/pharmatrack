package com.pharmatrack.inventario.domain;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "sucursal")
@Data
public class Sucursal {
  @Id
  @Column(name = "id_sucursal")
  private Integer id_sucursal;

  @Column(nullable = false)
  private String nombre;

  @Column(nullable = false)
  private String distrito;

  private String direccion;
}
