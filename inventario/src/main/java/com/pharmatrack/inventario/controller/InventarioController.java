package com.pharmatrack.inventario.controller;

import com.pharmatrack.inventario.domain.MovimientoStock;
import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.domain.Sucursal;
import com.pharmatrack.inventario.dto.AjusteStockRequest;
import com.pharmatrack.inventario.dto.SucursalCreateRequest;
import com.pharmatrack.inventario.dto.SucursalUpdateRequest;
import com.pharmatrack.inventario.infrastructure.service.InventarioService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
public class InventarioController {

  private final InventarioService svc;
  public InventarioController(InventarioService s) { this.svc = s; }

  // =======================
  // ====== Sucursales ======
  // =======================

  @GetMapping("/sucursales")
  public List<Sucursal> listarSucursales(@RequestParam(required = false) String distrito) {
    return svc.listarSucursales(distrito);
  }

  @GetMapping("/sucursales/{id_sucursal}")
  public Sucursal obtenerSucursalPorId(@PathVariable("id_sucursal") Integer id) {
    return svc.obtenerSucursalPorId(id);
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

  // =======================
  // ====== Stock ======
  // =======================

  @GetMapping("/stock")
  public List<Stock> consultarStock(
          @RequestParam(name = "id_producto", required = false) String idProductoParam,
          @RequestParam(name = "idProducto", required = false) String idProductoAlt,
          @RequestParam(name = "id_sucursal", required = false) Integer idSucursalParam,
          @RequestParam(name = "idSucursal", required = false) Integer idSucursalAlt,
          @RequestParam(required = false) String distrito) {

    String idProducto = Optional.ofNullable(idProductoParam).orElse(idProductoAlt);
    Integer idSucursal = Optional.ofNullable(idSucursalParam).orElse(idSucursalAlt);

    return svc.consultarStock(idProducto, idSucursal, distrito);
  }

  @PatchMapping("/stock")
  public Stock ajustarStock(@Valid @RequestBody AjusteStockRequest body) {
    return svc.ajustarStock(
            body.id_sucursal(),
            body.id_producto(),
            body.ajuste(),
            body.motivo()
    );
  }

  // =======================
  // ====== Movimientos ======
  // =======================

  @GetMapping("/movimientos")
  public List<MovimientoStock> listarMovimientos(
          @RequestParam(name="producto_id", required=false) String productoId,
          @RequestParam(name="sucursal_id", required=false) Integer sucursalId) {
    return svc.listarMovimientos(productoId, sucursalId);
  }

  @PostMapping("/movimientos")
  @ResponseStatus(HttpStatus.CREATED)
  public MovimientoStock crearMovimiento(@Valid @RequestBody MovimientoStock m) {
    return svc.registrarMovimiento(m);
  }
}