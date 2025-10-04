package com.pharmatrack.inventario.domain;

import java.io.Serializable;
import lombok.Data;

@Data
public class StockPK implements Serializable {
  private Integer id_sucursal;
  private String id_producto;
}
