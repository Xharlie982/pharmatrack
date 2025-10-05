package com.pharmatrack.inventario.dto;

import jakarta.validation.constraints.NotBlank;

public record SucursalUpdateRequest(
        @NotBlank String nombre,
        @NotBlank String distrito,
        @NotBlank String direccion
) {}
