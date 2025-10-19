package com.pharmatrack.inventario.infrastructure.client;

import com.pharmatrack.inventario.exception.ProductInactiveException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

@Component
public class CatalogoClient {

    private final RestTemplate rest;
    private final String baseUrl;
    private final boolean validate;

    public CatalogoClient(RestTemplateBuilder builder,
                          @Value("${catalogo.base-url}") String baseUrl,
                          @Value("${catalogo.validate-product:true}") boolean validate) {
        this.rest = builder
                .setConnectTimeout(Duration.ofSeconds(3))
                .setReadTimeout(Duration.ofSeconds(4))
                .build();
        this.baseUrl = baseUrl;
        this.validate = validate;
    }

    public boolean isEnabled() {
        return validate && baseUrl != null && !baseUrl.isBlank();
    }

    public void validarProducto(String id) {
        if (!isEnabled()) {
            return;
        }
        try {
            ResponseEntity<Map<String, Object>> resp = rest.exchange(
                    baseUrl + "/productos/{id}",
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {},
                    id);

            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                if (!Objects.equals(resp.getBody().get("activo"), true)) {
                    throw new ProductInactiveException("Operación rechazada: El producto '" + id + "' está descontinuado o inactivo.");
                }
            } else {
                throw new NoSuchElementException("Producto no encontrado en Catálogo: " + id);
            }

        } catch (HttpClientErrorException.NotFound e) {
            throw new NoSuchElementException("Producto no encontrado en Catálogo: " + id);
        } catch (RestClientException e) {
            throw new IllegalStateException("No se pudo validar con Catálogo", e);
        }
    }
}