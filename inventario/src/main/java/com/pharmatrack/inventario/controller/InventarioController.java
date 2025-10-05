package com.pharmatrack.inventario.controller;

import com.pharmatrack.inventario.domain.MovimientoStock;
import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.domain.Sucursal;
import com.pharmatrack.inventario.infrastructure.service.InventarioService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class InventarioController {

  private final InventarioService svc;

  public InventarioController(InventarioService s) { this.svc = s; }

  @GetMapping("/sucursales")
  public List<Sucursal> suc(@RequestParam(required = false) String distrito) {
    return svc.listarSucursales(distrito);
  }

  @GetMapping("/stock")
  public List<Stock> st(
          @RequestParam(name = "id_producto", required = false) String id_producto,
          @RequestParam(name = "idProducto", required = false) String idProductoAlt,
          @RequestParam(name = "id_sucursal", required = false) Integer id_sucursal,
          @RequestParam(name = "idSucursal", required = false) Integer idSucursalAlt,
          @RequestParam(required = false) String distrito) {

    String idProducto = (id_producto != null) ? id_producto : idProductoAlt;
    Integer idSucursal = (id_sucursal != null) ? id_sucursal : idSucursalAlt;

    return svc.consultarStock(idProducto, idSucursal, distrito);
  }

  @PatchMapping("/stock")
  public Stock ajustar(@RequestBody Map<String, Object> b) {
    Integer idSucursal = (Integer) b.getOrDefault("id_sucursal", b.get("idSucursal"));
    String idProducto  = (String)  b.getOrDefault("id_producto", b.get("idProducto"));
    Number deltaNum    = (Number)  b.get("delta");
    String motivo      = (String)  b.getOrDefault("motivo", "AJUSTE");

    if (idSucursal == null || idProducto == null || deltaNum == null) {
      throw new IllegalArgumentException("Campos requeridos: id_sucursal/idSucursal, id_producto/idProducto, delta");
    }
    int delta = deltaNum.intValue();
    return svc.ajustarStock(idSucursal, idProducto, delta, motivo);
  }

  @GetMapping("/movimientos")
  public List<MovimientoStock> listarMov(
          @RequestParam(name="producto_id", required=false) String productoId,
          @RequestParam(name="sucursal_id", required=false) Integer sucursalId) {
    return svc.listarMovimientos(productoId, sucursalId);
  }

  @PostMapping("/movimientos")
  @ResponseStatus(HttpStatus.CREATED)
  public MovimientoStock mov(@RequestBody MovimientoStock m) {
    return svc.registrarMovimiento(m);
  }
}
