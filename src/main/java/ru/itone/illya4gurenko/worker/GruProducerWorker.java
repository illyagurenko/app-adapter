package ru.itone.illya4gurenko.worker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.service.producer.GruProducerService;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * фоновый рабочий поток, выполняющий непрерывный цикл выгрузки данных и отправки в kafka.
 */
@Component
@Scope("prototype")
@RequiredArgsConstructor
@Slf4j
public class GruProducerWorker implements Runnable {

    private final GruProducerService gruProducerService;

    private final AtomicBoolean isActive = new AtomicBoolean(true);

    private String topicOut;
    private long delay;
    private int producerId;

    /**
     * инициализирует воркер целевым топиком, временем паузы между батчами и порядковым номером.
     */
    public void init(String topicOut, long delay, int producerId) {
        this.topicOut = topicOut;
        this.delay = delay;
        this.producerId = producerId;
    }

    /**
     * основной цикл работы потока: опрос бд, отправка в кафку и засыпание при отсутствии данных или сбое.
     */
    @Override
    public void run() {
        log.info("producer [{}] started for topic: {}", producerId, topicOut);

        while (isActive.get()) {
            try {
                boolean hasProcessedData = gruProducerService.produce(topicOut);

                if (!hasProcessedData) {
                    log.warn("producer [{}] found NO records or error occurred. Sleeping for {} ms...", producerId, delay);
                    Thread.sleep(delay);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("producer [{}] interrupted", producerId);
                break;

            } catch (Exception e) {
                log.error("producer [{}] technical error", producerId, e);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    log.info("producer [{}] interrupted during delay", producerId);
                    break;
                }
            }
        }

        log.info("producer [{}] stopped", producerId);
    }

    /**
     * выставляет флаг остановки для корректного завершения цикла потока.
     */
    public void stop() {
        log.info("stopping Producer [{}]...", producerId);
        isActive.set(false);
    }
}