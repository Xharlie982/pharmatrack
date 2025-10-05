package com.pharmatrack.inventario.infrastructure.repository;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.List;
import com.pharmatrack.inventario.domain.MovimientoStock;

public interface MovimientoRepo extends JpaRepository<MovimientoStock, Long> {

    @Query("""
    select m from MovimientoStock m
    where (:p is null or m.id_producto = :p)
      and (:s is null or m.id_sucursal = :s)
    order by m.fecha_movimiento desc
  """)
    List<MovimientoStock> buscar(@Param("p") String productoId,
                                 @Param("s") Integer sucursalId);
}
