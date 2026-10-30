package ru.itone.illya4gurenko.service.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * низкоуровневый компонент для синхронной отправки строковых сообщений в kafka с логированием смещения.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerSender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * отправляет сообщение в указанный топик kafka и ожидает подтверждения доставки.
     */
    public void send(String topic, String key, String payload) {
        try {
            SendResult<String, String> res = kafkaTemplate.send(topic, key, payload).get();
            log.info("Success produce in kafka topic: {}, partition: {}, offset: {}",
                    topic, res.getRecordMetadata().partition(), res.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kafka thread interrupted", e);
        } catch (Exception e) {
            log.error("Error sending to topic {}", topic, e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}