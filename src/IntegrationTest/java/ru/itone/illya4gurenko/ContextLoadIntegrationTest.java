package ru.itone.illya4gurenko;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import ru.itone.illya4gurenko.config.AppInitializer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class ContextLoadIntegrationTest {

    @MockBean
    private AppInitializer appInitializer;

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    @DisplayName("проверка поднятия контекста")
    void contextLoads() {
        assertNotNull(applicationContext);
    }
}
