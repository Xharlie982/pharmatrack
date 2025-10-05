package com.pharmatrack.inventario.infrastructure.service;

import com.pharmatrack.inventario.domain.MovimientoStock;
import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.domain.StockPK;
import com.pharmatrack.inventario.domain.Sucursal;
import com.pharmatrack.inventario.dto.SucursalCreateRequest;
import com.pharmatrack.inventario.dto.SucursalUpdateRequest;
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

  // ---------- Sucursales ----------

  public List<Sucursal> listarSucursales(String distrito) {
    if (distrito == null || distrito.isBlank()) return sucursalRepo.findAll();
    return sucursalRepo.findByDistritoIgnoreCase(distrito.trim());
  }

  @Transactional
  public Sucursal crearSucursal(SucursalCreateRequest req) {
    if (sucursalRepo.existsById(req.id_sucursal())) {
      throw new IllegalArgumentException("La sucursal ya existe");
    }
    Sucursal s = new Sucursal();
    s.setId_sucursal(req.id_sucursal());
    s.setNombre(req.nombre().trim());
    s.setDistrito(req.distrito().trim());
    s.setDireccion(req.direccion().trim());
    return sucursalRepo.save(s);
  }

  @Transactional
  public Sucursal actualizarSucursal(Integer id, SucursalUpdateRequest req) {
    Sucursal s = sucursalRepo.findById(id)
            .orElseThrow(() -> new NoSuchElementException("No existe"));
    s.setNombre(req.nombre().trim());
    s.setDistrito(req.distrito().trim());
    s.setDireccion(req.direccion().trim());
    return sucursalRepo.save(s);
  }

  // ---------- Stock ----------

  public List<Stock> consultarStock(String idProducto, Integer idSucursal, String distrito) {
    List<Integer> idsPorDistrito = null;
    if (distrito != null && !distrito.isBlank()) {
      idsPorDistrito = sucursalRepo.findByDistritoIgnoreCase(distrito.trim())
              .stream().map(Sucursal::getId_sucursal).collect(Collectors.toList());
      if (idsPorDistrito.isEmpty()) return List.of();
    }

    if (idProducto != null && idSucursal != null) {
      StockPK pk = new StockPK();
      pk.setId_sucursal(idSucursal);
      pk.setId_producto(idProducto);
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
  public Stock ajustarStock(Integer idSucursal, String idProducto, int ajuste, String motivo) {
    if (ajuste == 0) throw new IllegalArgumentException("ajuste no puede ser 0");

    StockPK pk = new StockPK();
    pk.setId_sucursal(idSucursal);
    pk.setId_producto(idProducto);

    Stock s = stockRepo.findById(pk).orElseThrow(() ->
            new NoSuchElementException("No existe stock para esa sucursal y producto"));

    int nuevo = s.getStock_actual() + ajuste;
    if (nuevo < 0) throw new IllegalArgumentException("No hay stock suficiente para egreso");

    s.setStock_actual(nuevo);
    s.setFecha_actualizacion(OffsetDateTime.now());
    stockRepo.save(s);

    MovimientoStock m = new MovimientoStock();
    m.setId_sucursal(idSucursal);
    m.setId_producto(idProducto);
    m.setTipo_movimiento(ajuste >= 0 ? "ENTRADA" : "EGRESO");
    m.setCantidad(Math.abs(ajuste));
    m.setMotivo(motivo);
    movimientoRepo.save(m);

    return s;
  }

  // ---------- Movimientos ----------

  @Transactional
  public MovimientoStock registrarMovimiento(MovimientoStock m) {
    if (m.getId_sucursal() == null || m.getId_producto() == null ||
            m.getTipo_movimiento() == null || m.getCantidad() == null) {
      throw new IllegalArgumentException("Campos requeridos: id_sucursal, id_producto, tipo_movimiento, cantidad");
    }

    String tipo = m.getTipo_movimiento().toUpperCase(Locale.ROOT);
    if (!tipo.equals("ENTRADA") && !tipo.equals("EGRESO")) {
      throw new IllegalArgumentException("tipo_movimiento debe ser ENTRADA o EGRESO");
    }
    if (m.getCantidad() <= 0) throw new IllegalArgumentException("cantidad debe ser > 0");

    int delta = tipo.equals("ENTRADA") ? m.getCantidad() : -m.getCantidad();

    StockPK pk = new StockPK();
    pk.setId_sucursal(m.getId_sucursal());
    pk.setId_producto(m.getId_producto());

    Stock s = stockRepo.findById(pk).orElse(null);

    // Atajo controlado: si es ENTRADA y no existe fila, la creamos
    if (s == null) {
      if ("ENTRADA".equals(tipo)) {
        s = new Stock();
        s.setId_sucursal(m.getId_sucursal());
        s.setId_producto(m.getId_producto());
        s.setStock_actual(0);
        s.setUmbral_reposicion(0);
        s.setFecha_actualizacion(OffsetDateTime.now());
      } else {
        throw new NoSuchElementException("No existe stock para esa sucursal y producto");
      }
    }

    int nuevo = s.getStock_actual() + delta;
    if (nuevo < 0) throw new IllegalArgumentException("No hay stock suficiente para egreso");

    s.setStock_actual(nuevo);
    s.setFecha_actualizacion(OffsetDateTime.now());
    stockRepo.save(s);

    m.setTipo_movimiento(tipo);
    return movimientoRepo.save(m);
  }

  public List<MovimientoStock> listarMovimientos(String productoId, Integer sucursalId) {
    return movimientoRepo.buscar(productoId, sucursalId);
  }
}
