package com.pharmatrack.inventario.infrastructure.service;

import com.pharmatrack.inventario.domain.MovimientoStock;
import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.domain.StockPK;
import com.pharmatrack.inventario.domain.Sucursal;
import com.pharmatrack.inventario.infrastructure.repository.MovimientoRepo;
import com.pharmatrack.inventario.infrastructure.repository.StockRepo;
import com.pharmatrack.inventario.infrastructure.repository.SucursalRepo;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class InventarioService {

  private final SucursalRepo sucursalRepo;
  private final StockRepo stockRepo;
  private final MovimientoRepo movimientoRepo;

  public InventarioService(SucursalRepo sr, StockRepo str, MovimientoRepo mr) {
    this.sucursalRepo = sr;
    this.stockRepo = str;
    this.movimientoRepo = mr;
  }

  public List<Sucursal> listarSucursales(String distrito) {
    if (distrito == null || distrito.isBlank()) return sucursalRepo.findAll();
    return sucursalRepo.findByDistritoIgnoreCase(distrito.trim());
  }

  public List<Stock> consultarStock(String idProducto, Integer idSucursal, String distrito) {
    List<Integer> idsPorDistrito = null;
    if (distrito != null && !distrito.isBlank()) {
      idsPorDistrito = sucursalRepo.findByDistritoIgnoreCase(distrito.trim())
              .stream().map(Sucursal::getId_sucursal).collect(Collectors.toList());
      if (idsPorDistrito.isEmpty()) return List.of();
    }

    if (idProducto != null && idSucursal != null) {
      StockPK pk = new StockPK(); pk.setId_sucursal(idSucursal); pk.setId_producto(idProducto);
      return stockRepo.findById(pk).map(List::of).orElse(List.of());
    }

    if (idsPorDistrito != null) {
      if (idProducto != null) return stockRepo.findByProductoAndSucursales(idProducto, idsPorDistrito);
      return stockRepo.findBySucursales(idsPorDistrito);
    }

    if (idSucursal != null) return stockRepo.findBySucursal(idSucursal);
    if (idProducto != null) return stockRepo.findByProducto(idProducto);

    return stockRepo.findAll();
  }

  @Transactional
  public Stock ajustarStock(Integer idSucursal, String idProducto, int delta, String motivo) {
    if (delta == 0) throw new IllegalArgumentException("delta no puede ser 0");

    StockPK pk = new StockPK(); pk.setId_sucursal(idSucursal); pk.setId_producto(idProducto);
    Stock s = stockRepo.findById(pk).orElseThrow(() ->
            new NoSuchElementException("No existe stock para esa sucursal y producto"));

    int nuevo = s.getStock_actual() + delta;
    if (nuevo < 0) throw new IllegalArgumentException("No hay stock suficiente para egreso");

    s.setStock_actual(nuevo);
    s.setFecha_actualizacion(OffsetDateTime.now());
    stockRepo.save(s);

    MovimientoStock m = new MovimientoStock();
    m.setId_sucursal(idSucursal);
    m.setId_producto(idProducto);
    m.setTipo_movimiento(delta >= 0 ? "ENTRADA" : "EGRESO");
    m.setCantidad(Math.abs(delta));
    m.setMotivo(motivo);
    movimientoRepo.save(m);

    return s;
  }

  @Transactional
  public MovimientoStock registrarMovimiento(MovimientoStock m) {
    if (m.getId_sucursal() == null || m.getId_producto() == null ||
            m.getTipo_movimiento() == null || m.getCantidad() == null) {
      throw new IllegalArgumentException("Campos requeridos: id_sucursal, id_producto, tipo_movimiento, cantidad");
    }
    String tipo = m.getTipo_movimiento().toUpperCase(Locale.ROOT);
    if (!tipo.equals("ENTRADA") && !tipo.equals("EGRESO"))
      throw new IllegalArgumentException("tipo_movimiento debe ser ENTRADA o EGRESO");
    if (m.getCantidad() <= 0) throw new IllegalArgumentException("cantidad debe ser > 0");

    // Aplica al stock
    int delta = tipo.equals("ENTRADA") ? m.getCantidad() : -m.getCantidad();

    StockPK pk = new StockPK(); pk.setId_sucursal(m.getId_sucursal()); pk.setId_producto(m.getId_producto());
    Stock s = stockRepo.findById(pk).orElseThrow(() ->
            new NoSuchElementException("No existe stock para esa sucursal y producto"));

    int nuevo = s.getStock_actual() + delta;
    if (nuevo < 0) throw new IllegalArgumentException("No hay stock suficiente para egreso");

    s.setStock_actual(nuevo);
    s.setFecha_actualizacion(OffsetDateTime.now());
    stockRepo.save(s);

    // Guarda el movimiento (fecha la pone la DB)
    m.setTipo_movimiento(tipo);
    return movimientoRepo.save(m);
  }

  public List<MovimientoStock> listarMovimientos(String productoId, Integer sucursalId) {
    return movimientoRepo.buscar(productoId, sucursalId);
  }
}
