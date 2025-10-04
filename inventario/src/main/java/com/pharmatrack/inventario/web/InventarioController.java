package com.pharmatrack.inventario.web;

import org.springframework.web.bind.annotation.*;
import java.util.*;
import com.pharmatrack.inventario.service.*;
import com.pharmatrack.inventario.domain.*;

@RestController
public class InventarioController {
  private final InventarioService svc;
  public InventarioController(InventarioService s){ this.svc = s; }

  @GetMapping("/sucursales")
  public List<Sucursal> suc(@RequestParam(required=false) String distrito) {
    return svc.listarSucursales(distrito);
  }

  @GetMapping("/stock")
  public List<Stock> st(@RequestParam(name="id_producto", required=false) String idProducto,
                        @RequestParam(name="id_sucursal", required=false) Integer idSucursal,
                        @RequestParam(required=false) String distrito) {
    return svc.consultarStock(idProducto, idSucursal, distrito);
  }

  @PatchMapping("/stock")
  public Stock ajustar(@RequestBody Map<String,Object> b){
    return svc.ajustarStock(
      (Integer)b.get("id_sucursal"),
      (String)b.get("id_producto"),
      ((Number)b.get("delta")).intValue(),
      (String)b.getOrDefault("motivo","AJUSTE")
    );
  }

  @PostMapping("/movimientos")
  @ResponseStatus(org.springframework.http.HttpStatus.CREATED)
  public MovimientoStock mov(@RequestBody MovimientoStock m) {
    return svc.registrarMovimiento(m);
  }
}
