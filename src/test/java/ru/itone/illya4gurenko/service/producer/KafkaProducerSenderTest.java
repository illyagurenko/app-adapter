package ru.itone.illya4gurenko.service.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class KafkaProducerSenderTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
    private final KafkaProducerSender sender = new KafkaProducerSender(kafkaTemplate);

    @Test
    @DisplayName("успешная отправка сообщения в топик kafka")
    void testSendSuccess() {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("t1", 0), 0, 0, 0, 0, 0);
        SendResult<String, String> sendResult = new SendResult<>(null, metadata);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(), any(), any())).thenReturn(future);

        assertDoesNotThrow(() -> sender.send("t1", "k1", "payload"));
    }

    @Test
    @DisplayName("ошибка при отправке сообщения в топик kafka")
    void testSendFailure() {
        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Delivery failed"));

        when(kafkaTemplate.send(any(), any(), any())).thenReturn(future);

        assertThrows(RuntimeException.class, () -> sender.send("t1", "k1", "payload"));
    }
}

