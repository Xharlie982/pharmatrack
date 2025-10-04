package com.pharmatrack.inventario.service;

import org.springframework.data.jpa.repository.JpaRepository;
import com.pharmatrack.inventario.domain.MovimientoStock;

public interface MovimientoRepo extends JpaRepository<MovimientoStock, Long> {}
