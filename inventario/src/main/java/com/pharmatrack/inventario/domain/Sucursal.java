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

  private String nombre;
  private String distrito;
  private String direccion;
}
