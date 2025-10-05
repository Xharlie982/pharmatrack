package com.pharmatrack.inventario.controller;

import com.pharmatrack.inventario.domain.MovimientoStock;
import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.domain.Sucursal;
import com.pharmatrack.inventario.dto.SucursalCreateRequest;
import com.pharmatrack.inventario.dto.SucursalUpdateRequest;
import com.pharmatrack.inventario.infrastructure.service.InventarioService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class InventarioController {

  private final InventarioService svc;

  public InventarioController(InventarioService s) { this.svc = s; }

  // ---------- Sucursales ----------
  @GetMapping("/sucursales")
  public List<Sucursal> listarSucursales(@RequestParam(required = false) String distrito) {
    return svc.listarSucursales(distrito);
  }

  @PostMapping("/sucursales")
  @ResponseStatus(HttpStatus.CREATED)
  public Sucursal crearSucursal(@Valid @RequestBody SucursalCreateRequest req) {
    return svc.crearSucursal(req);
  }

  @PutMapping("/sucursales/{id_sucursal}")
  public Sucursal actualizarSucursal(@PathVariable("id_sucursal") Integer id,
                                     @Valid @RequestBody SucursalUpdateRequest req) {
    return svc.actualizarSucursal(id, req);
  }

  // ---------- Stock ----------
  @GetMapping("/stock")
  public List<Stock> consultarStock(
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
  public Stock ajustarStock(@RequestBody Map<String, Object> body) {
    Integer idSucursal = (Integer) body.getOrDefault("id_sucursal", body.get("idSucursal"));
    String idProducto  = (String)  body.getOrDefault("id_producto", body.get("idProducto"));

    // Nombre preferido: "ajuste". Compatibilidad con "delta".
    Number ajusteNum   = (Number) (body.get("ajuste") != null ? body.get("ajuste") : body.get("delta"));
    String motivo      = (String)  body.getOrDefault("motivo", "AJUSTE");

    if (idSucursal == null || idProducto == null || ajusteNum == null) {
      throw new IllegalArgumentException("Campos requeridos: id_sucursal/idSucursal, id_producto/idProducto, ajuste (o delta)");
    }
    int ajuste = ajusteNum.intValue();
    return svc.ajustarStock(idSucursal, idProducto, ajuste, motivo);
  }

  // ---------- Movimientos ----------
  @GetMapping("/movimientos")
  public List<MovimientoStock> listarMovimientos(
          @RequestParam(name="producto_id", required=false) String productoId,
          @RequestParam(name="sucursal_id", required=false) Integer sucursalId) {
    return svc.listarMovimientos(productoId, sucursalId);
  }

  @PostMapping("/movimientos")
  @ResponseStatus(HttpStatus.CREATED)
  public MovimientoStock crearMovimiento(@RequestBody MovimientoStock m) {
    return svc.registrarMovimiento(m);
  }
}
