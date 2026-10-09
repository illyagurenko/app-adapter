package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Scope("prototype")
@RequiredArgsConstructor
@Slf4j
public class GruProducerKafka implements Runnable {

    private final GruProducerService gruProducerService;
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    private String topicOut;
    private long delay;
    private int producerId;

    public void init(String topicOut, long delay, int producerId) {
        this.topicOut = topicOut;
        this.delay = delay;
        this.producerId = producerId;
    }

    @Override
    public void run() {
        log.info("Producer [{}] started for topic: {}", producerId, topicOut);

        while (isActive.get()) {
            try {
                boolean hasProcessedData = gruProducerService.produce(topicOut);

                if (!hasProcessedData) {
                    Thread.sleep(delay);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Producer [{}] interrupted", producerId);
                break;
            } catch (Exception e) {
                log.error("Producer [{}] tech error", producerId, e);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void stop() {
        isActive.set(false);
    }
}