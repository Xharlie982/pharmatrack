package com.pharmatrack.inventario.infrastructure.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Component
public class CatalogoClient {

    private final RestTemplate rest;
    private final String baseUrl;
    private final boolean validate;

    public CatalogoClient(RestTemplateBuilder builder,
                          @Value("${catalogo.base-url:http://localhost:8084/catalogo}") String baseUrl,
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

    public boolean existeProducto(String id) {
        if (!isEnabled()) return true;
        try {
            ResponseEntity<Void> resp =
                    rest.getForEntity(baseUrl + "/productos/{id}", Void.class, id);
            return resp.getStatusCode().is2xxSuccessful();
        } catch (HttpClientErrorException.NotFound e) {
            return false;
        } catch (RestClientException e) {
            throw new IllegalStateException("No se pudo validar con Catálogo", e);
        }
    }
}