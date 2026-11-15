package ru.itone.illya4gurenko.service.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.ConsumerFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class AppKafkaConsumerTest {

    @SuppressWarnings("unchecked")
    private final ConsumerFactory<String, String> consumerFactory = Mockito.mock(ConsumerFactory.class);
    private final AppKafkaConsumer consumer = new AppKafkaConsumer(consumerFactory);

    @Test
    @DisplayName("остановка незапущенного kafka консьюмера")
    void testStopWhenNotStarted() {
        assertDoesNotThrow(consumer::stop);
    }
}
