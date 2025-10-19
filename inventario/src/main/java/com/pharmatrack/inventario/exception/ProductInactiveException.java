package com.pharmatrack.inventario.exception;

public class ProductInactiveException extends RuntimeException {
    public ProductInactiveException(String message) {
        super(message);
    }
}