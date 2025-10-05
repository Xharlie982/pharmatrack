package com.pharmatrack.inventario.dto;

import jakarta.validation.constraints.*;

public record SucursalCreateRequest(
        @NotNull @Positive Integer id_sucursal,
        @NotBlank String nombre,
        @NotBlank String distrito,
        @NotBlank String direccion
) {}
