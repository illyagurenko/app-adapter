package ru.itone.illya4gurenko.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;
import ru.itone.illya4gurenko.config.AppInitializer;
import ru.itone.illya4gurenko.controller.dto.AdminResponse;
import ru.itone.illya4gurenko.controller.dto.ConsumerMetricsResponse;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdminControllerTest {

    private final AppInitializer appInitializer = Mockito.mock(AppInitializer.class);
    private final AppAdapterTransDao transDao = Mockito.mock(AppAdapterTransDao.class);
    private final AdminController controller = new AdminController(appInitializer, transDao);

    @Test
    @DisplayName("получение статуса работы адаптера")
    void testStatus() {
        when(appInitializer.isRunning()).thenReturn(true);
        when(appInitializer.getProducerCount()).thenReturn(2);
        when(appInitializer.getConsumerCount()).thenReturn(2);

        ResponseEntity<AdminResponse> response = controller.status();

        assertAll(
                () -> assertEquals(200, response.getStatusCode().value()),
                () -> assertNotNull(response.getBody()),
                () -> assertEquals(true, response.getBody().running())
        );
    }

    @Test
    @DisplayName("получение пустых метрик консьюмера")
    void testMetricsWhenEmpty() {
        when(transDao.getMetrics()).thenReturn(null);

        ResponseEntity<ConsumerMetricsResponse> response = controller.metrics();

        assertAll(
                () -> assertEquals(200, response.getStatusCode().value()),
                () -> assertNotNull(response.getBody()),
                () -> assertEquals(0, response.getBody().totalCompleted())
        );
    }

    @Test
    @DisplayName("получение заполненных метрик консьюмера")
    void testMetricsWhenPopulated() {
        ConsumerMetricsResponse raw = new ConsumerMetricsResponse(
                10,
                LocalDateTime.now().minusMinutes(2),
                LocalDateTime.now(),
                125.5,
                "",
                12.5,
                15.0,
                0.0,
                0
        );
        when(transDao.getMetrics()).thenReturn(raw);

        ResponseEntity<ConsumerMetricsResponse> response = controller.metrics();

        assertAll(
                () -> assertEquals(200, response.getStatusCode().value()),
                () -> assertNotNull(response.getBody()),
                () -> assertEquals(10, response.getBody().totalCompleted()),
                () -> assertEquals(1000L, response.getBody().estimatedRowsProcessed())
        );
    }

    @Test
    @DisplayName("перезапуск воркеров через контроллер")
    void testRestart() {
        ResponseEntity<AdminResponse> response = controller.restart();

        assertAll(
                () -> assertEquals(200, response.getStatusCode().value()),
                () -> verify(appInitializer).restartWorkers()
        );
    }

    @Test
    @DisplayName("остановка воркеров через контроллер")
    void testStopWorkers() {
        ResponseEntity<AdminResponse> response = controller.stopWorkers();

        assertAll(
                () -> assertEquals(200, response.getStatusCode().value()),
                () -> verify(appInitializer).stopWorkers()
        );
    }
}

