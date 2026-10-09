package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
@RequiredArgsConstructor
@Slf4j
public class GruConsumerKafka {

    private final ConsumerFactory<String, String> consumerFactory;
    private final GruConsumerService gruConsumerService;

    private KafkaMessageListenerContainer<String, String> container;
    private int consumerId;

    public void start(String topicIn, int consumerId) {
        this.consumerId = consumerId;
        log.info("Starting Consumer [{}] for topic: {}", consumerId, topicIn);

        ContainerProperties properties = new ContainerProperties(topicIn);

        properties.setMessageListener((MessageListener<String, String>) record -> {
            try {
                log.info("Consumer [{}] got message: key={}, offset={}", consumerId, record.key(), record.offset());
                gruConsumerService.consume(record.value());
            } catch (Exception e) {
                log.error("Consumer [{}] error processing message", consumerId, e);
            }
        });

        container = new KafkaMessageListenerContainer<>(consumerFactory, properties);
        container.setBeanName("consumer-vista-container-" + consumerId);
        container.start();

        log.info("Consumer [{}] container started successfully", consumerId);
    }

    public void stop() {
        if (container != null) {
            log.info("Stopping Consumer [{}]...", consumerId);
            container.stop();
        }
    }
}