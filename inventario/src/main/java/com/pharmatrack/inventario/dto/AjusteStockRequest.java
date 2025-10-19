package com.pharmatrack.inventario.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record AjusteStockRequest(
        @NotNull Integer id_sucursal,
        @NotBlank String id_producto,
        @NotNull Integer ajuste,
        String motivo
) {
    public AjusteStockRequest {
        if (motivo == null || motivo.isBlank()) {
            motivo = "AJUSTE";
        }
    }
}