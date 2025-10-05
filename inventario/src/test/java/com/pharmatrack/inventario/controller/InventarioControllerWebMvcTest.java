package com.pharmatrack.inventario.controller;

import com.pharmatrack.inventario.domain.Stock;
import com.pharmatrack.inventario.infrastructure.service.InventarioService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.OffsetDateTime;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(InventarioController.class)
class InventarioControllerWebMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private InventarioService inventarioService;

    @Test
    void debeListarStockPorProductoYSucursal() throws Exception {
        Stock s = new Stock();
        s.setId_producto("P001");
        s.setId_sucursal(10);
        s.setStock_actual(25);
        s.setUmbral_reposicion(5);
        s.setFecha_actualizacion(OffsetDateTime.now());

        Mockito.when(inventarioService.consultarStock(eq("P001"), eq(10), isNull()))
                .thenReturn(List.of(s));

        mockMvc.perform(get("/inventario/stock")
                        .contextPath("/inventario")          // 👈 clave
                        .param("id_producto", "P001")        // usa snake_case como en el controller
                        .param("id_sucursal", "10")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id_producto").value("P001"))
                .andExpect(jsonPath("$[0].id_sucursal").value(10))
                .andExpect(jsonPath("$[0].stock_actual").value(25));
    }

    @Test
    void debeListarStockFiltrandoPorDistrito() throws Exception {
        Stock s1 = new Stock();
        s1.setId_producto("P002");
        s1.setId_sucursal(20);
        s1.setStock_actual(7);
        s1.setUmbral_reposicion(3);
        s1.setFecha_actualizacion(OffsetDateTime.now());

        Mockito.when(inventarioService.consultarStock(isNull(), isNull(), eq("LIMA")))
                .thenReturn(List.of(s1));

        mockMvc.perform(get("/inventario/stock")
                        .contextPath("/inventario")          // 👈 clave
                        .param("distrito", "LIMA")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id_producto").value("P002"))
                .andExpect(jsonPath("$[0].id_sucursal").value(20))
                .andExpect(jsonPath("$[0].stock_actual").value(7));
    }
}
