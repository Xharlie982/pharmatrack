package com.pharmatrack.inventario.service;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.List;
import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.domain.StockPK;

public interface StockRepo extends JpaRepository<Stock, StockPK> {

  @Query("select s from Stock s where s.id_producto = :p")
  List<Stock> findByProducto(@Param("p") String p);

  @Query("select s from Stock s where s.id_sucursal = :id")
  List<Stock> findBySucursal(@Param("id") Integer id);

  @Query("select s from Stock s where s.id_sucursal in :ids")
  List<Stock> findBySucursales(@Param("ids") List<Integer> ids);

  @Query("select s from Stock s where s.id_producto = :p and s.id_sucursal in :ids")
  List<Stock> findByProductoAndSucursales(@Param("p") String p, @Param("ids") List<Integer> ids);
}
