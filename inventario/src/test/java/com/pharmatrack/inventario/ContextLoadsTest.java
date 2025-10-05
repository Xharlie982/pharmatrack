package com.pharmatrack.inventario;

import com.pharmatrack.inventario.infrastructure.repository.MovimientoRepo;
import com.pharmatrack.inventario.infrastructure.repository.StockRepo;
import com.pharmatrack.inventario.infrastructure.repository.SucursalRepo;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
        // Evita autoconfig de DataSource/JPA y usa mocks de repos
        "spring.autoconfigure.exclude=" +
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration," +
                "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
class ContextLoadsTest {

    @MockBean SucursalRepo sucursalRepo;
    @MockBean StockRepo stockRepo;
    @MockBean MovimientoRepo movimientoRepo;

    @Test
    void contextLoads() {
        // Si el contexto arranca, el test pasa.
    }
}
