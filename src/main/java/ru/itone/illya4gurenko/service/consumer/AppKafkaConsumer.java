package ru.itone.illya4gurenko.service.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * универсальный прототип-компонент контейнера kafka-консьюмера с ручным подтверждением смещений (MANUAL_IMMEDIATE).
 */
@Component
@Scope("prototype")
@RequiredArgsConstructor
@Slf4j
public class AppKafkaConsumer {

    private final ConsumerFactory<String, String> consumerFactory;
    private KafkaMessageListenerContainer<String, String> container;
    private int consumerId;

    /**
     * выполняет запуск слушателя для переданного топика и связывает его с целевым обработчиком.
     */
    public void start(
            String topicIn,
            int consumerId,
            MessageConsumer messageConsumer
    ) {
        this.consumerId = consumerId;

        log.info("starting consumer [{}] for topic: {}", consumerId, topicIn);

        ContainerProperties properties = new ContainerProperties(topicIn);
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        properties.setSyncCommits(true);

        properties.setMessageListener(
                (AcknowledgingMessageListener<String, String>) (record, acknowledgment) -> {
                    log.info("consumer [{}] got message: topic={}, partition={}, offset={}, key={}",
                            consumerId, record.topic(), record.partition(), record.offset(), record.key());
                    try {
                        messageConsumer.consume(record.value());

                        acknowledgment.acknowledge();
                        log.info("consumer [{}] successfully processed and acknowledged. partition={}, offset={}",
                                consumerId, record.partition(), record.offset());

                    } catch (RuntimeException e) {
                        log.error("consumer [{}] failed processing message. partition={}, offset={}",
                                consumerId, record.partition(), record.offset(), e);
                        throw e;
                    }
                }
        );

        container = new KafkaMessageListenerContainer<>(consumerFactory, properties);
        container.setBeanName("kafka-listener-" + topicIn + "-" + consumerId);
        container.start();

        log.info("consumer [{}] container started successfully for topic: {}", consumerId, topicIn);
    }

    /**
     * останавливает работу контейнера слушателя kafka.
     */
    public void stop() {
        if (container != null) {
            log.info("stopping Consumer [{}]...", consumerId);
            container.stop();
            log.info("consumer [{}] stopped", consumerId);
        }
    }
}