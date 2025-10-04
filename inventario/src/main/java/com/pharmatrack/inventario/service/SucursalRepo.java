package com.pharmatrack.inventario.service;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import com.pharmatrack.inventario.domain.Sucursal;

public interface SucursalRepo extends JpaRepository<Sucursal, Integer> {
  List<Sucursal> findByDistritoIgnoreCase(String d);
}
