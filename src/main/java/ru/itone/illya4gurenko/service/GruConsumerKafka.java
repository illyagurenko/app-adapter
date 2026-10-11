package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
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

    public void start(
            String topicIn,
            int consumerId
    ) {

        this.consumerId = consumerId;

        log.info("starting consumer [{}] for topic: {}", consumerId, topicIn);

        ContainerProperties properties =
                new ContainerProperties(topicIn);

        properties.setAckMode(
                ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );

        properties.setSyncCommits(true);

        properties.setMessageListener(
                (AcknowledgingMessageListener<String, String>)
                        (record, acknowledgment) -> {
                            log.info("consumer [{}] got message: " +
                                            "topic={}, partition={}, offset={}, key={}",
                                    consumerId,
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key());
                            try {
                                gruConsumerService.consume(record.value());

                                acknowledgment.acknowledge();
                                log.info("consumer [{}] successfully processed " +
                                                "and acknowledged message. " +
                                                "partition={}, offset={}",
                                        consumerId, record.partition(), record.offset());

                            } catch (RuntimeException e) {
                                log.error("consumer [{}] failed processing message. " +
                                                "Message was NOT acknowledged. " +
                                                "partition={}, offset={}",
                                        consumerId, record.partition(), record.offset(), e);

                                throw e;
                            }
                        }
        );
        container = new KafkaMessageListenerContainer<>(consumerFactory, properties);

        container.setBeanName("consumer-vista-container-" + consumerId);

        container.start();
        log.info("consumer [{}] container started successfully", consumerId);
    }

    public void stop() {
        if (container == null) {
            return;
        }

        log.info("stopping Consumer [{}]...", consumerId);

        container.stop();

        log.info("consumer [{}] stopped", consumerId);
    }
}