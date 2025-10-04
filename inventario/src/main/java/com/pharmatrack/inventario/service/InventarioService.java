package com.pharmatrack.inventario.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.*;
import java.util.*;
import com.pharmatrack.inventario.domain.*;

@Service
@Transactional
public class InventarioService {
  private final StockRepo stock;
  private final MovimientoRepo mov;
  private final SucursalRepo suc;

  public InventarioService(StockRepo s, MovimientoRepo m, SucursalRepo su) {
    stock = s; mov = m; suc = su;
  }

  public List<Sucursal> listarSucursales(String distrito) {
    return (distrito == null || distrito.isBlank())
      ? suc.findAll()
      : suc.findByDistritoIgnoreCase(distrito);
  }

  public List<Stock> consultarStock(String idProducto, Integer idSucursal, String distrito) {
    // producto + distrito
    if (idProducto != null && distrito != null && !distrito.isBlank()) {
      var ids = suc.findByDistritoIgnoreCase(distrito).stream().map(Sucursal::getId_sucursal).toList();
      return ids.isEmpty() ? List.of() : stock.findByProductoAndSucursales(idProducto, ids);
    }
    // por distrito
    if (distrito != null && !distrito.isBlank()) {
      var ids = suc.findByDistritoIgnoreCase(distrito).stream().map(Sucursal::getId_sucursal).toList();
      return ids.isEmpty() ? List.of() : stock.findBySucursales(ids);
    }
    // por producto
    if (idProducto != null) return stock.findByProducto(idProducto);
    // por sucursal
    if (idSucursal != null) return stock.findBySucursal(idSucursal);
    // todo
    return stock.findAll();
  }

  public MovimientoStock registrarMovimiento(MovimientoStock req) {
    var pk = new StockPK();
    pk.setId_sucursal(req.getId_sucursal());
    pk.setId_producto(req.getId_producto());

    var s = stock.findById(pk).orElseGet(() -> {
      var ns = new Stock();
      ns.setId_sucursal(req.getId_sucursal());
      ns.setId_producto(req.getId_producto());
      ns.setStock_actual(0);
      ns.setUmbral_reposicion(0);
      ns.setFecha_actualizacion(OffsetDateTime.now(ZoneOffset.UTC));
      return ns;
    });

    int delta = "ENTRADA".equalsIgnoreCase(req.getTipo_movimiento())
      ? req.getCantidad()
      : -req.getCantidad();

    int nuevo = s.getStock_actual() + delta;
    if (nuevo < 0) throw new IllegalArgumentException("Stock insuficiente");

    s.setStock_actual(nuevo);
    s.setFecha_actualizacion(OffsetDateTime.now(ZoneOffset.UTC));
    stock.save(s);

    req.setFecha_movimiento(OffsetDateTime.now(ZoneOffset.UTC));
    return mov.save(req);
  }

  public Stock ajustarStock(Integer idSucursal, String idProducto, int delta, String motivo) {
    var m = new MovimientoStock();
    m.setId_sucursal(idSucursal);
    m.setId_producto(idProducto);
    m.setTipo_movimiento(delta >= 0 ? "ENTRADA" : "EGRESO");
    m.setCantidad(Math.abs(delta));
    m.setMotivo(motivo == null ? "AJUSTE" : motivo);
    registrarMovimiento(m);

    var pk = new StockPK();
    pk.setId_sucursal(idSucursal);
    pk.setId_producto(idProducto);
    return stock.findById(pk).orElseThrow();
  }
}
