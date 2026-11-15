package ru.itone.illya4gurenko.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.service.producer.GruProducerService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class GruProducerWorkerTest {

    private final GruProducerService producerService = Mockito.mock(GruProducerService.class);
    private final GruProducerWorker worker = new GruProducerWorker(producerService);

    @Test
    @DisplayName("инициализация и остановка воркера продюсера")
    void testInitAndStop() {
        worker.init("topic-out", 100, 1);
        worker.stop();
        assertDoesNotThrow(worker::run);
    }
}

