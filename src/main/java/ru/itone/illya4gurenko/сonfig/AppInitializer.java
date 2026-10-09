package ru.itone.illya4gurenko.сonfig;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.dao.AppAdapterConfigDao;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.service.GruConsumerKafka;
import ru.itone.illya4gurenko.service.GruProducerKafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppInitializer implements CommandLineRunner {

    private static final String SYSTEM_ID = "GRU";

    private final AppAdapterConfigDao configDao;
    private final ApplicationContext applicationContext;

    @Value("${batch.timeout.worker.delay:10000}")
    private long delay;

    @Value("${app.adapter.producer.pool-size:1}")
    private int producerPoolSize;

    @Value("${app.adapter.consumer.pool-size:1}")
    private int consumerPoolSize;

    private final Object lifecycleMonitor = new Object();

    private final List<GruProducerKafka> producers = new ArrayList<>();
    private final List<GruConsumerKafka> consumers = new ArrayList<>();

    private ExecutorService producerExecutor;

    private volatile boolean running = false;

    @Override
    public void run(String... args) {
        startWorkers();
    }

    /**
     * Читает актуальный APP_ADAPTER_CONFIG и запускает workers.
     */
    public void startWorkers() {
        synchronized (lifecycleMonitor) {
            startWorkersInternal();
        }
    }

    /**
     * Останавливает текущие workers, перечитывает APP_ADAPTER_CONFIG
     * и создаёт producer/consumer заново.
     */
    public void restartWorkers() {
        synchronized (lifecycleMonitor) {
            log.info("Restarting app-adapter workers...");

            stopWorkersInternal();
            startWorkersInternal();

            log.info(
                    "App-adapter workers restarted. Producers={}, Consumers={}",
                    producers.size(),
                    consumers.size()
            );
        }
    }

    /**
     * Останавливает только рабочие producer/consumer потоки.
     * Spring Boot / REST / DataSource продолжают работать.
     */
    public void stopWorkers() {
        synchronized (lifecycleMonitor) {
            stopWorkersInternal();
        }
    }

    private void startWorkersInternal() {
        if (running) {
            log.warn("Workers are already running");
            return;
        }

        log.info("Loading APP_ADAPTER_CONFIG for systemId={}", SYSTEM_ID);

        AppAdapterConfig config = configDao.getBySystemId(SYSTEM_ID);

        if (config == null) {
            throw new IllegalStateException(
                    "APP_ADAPTER_CONFIG for systemId='" + SYSTEM_ID + "' not found"
            );
        }

        log.info(
                "APP_ADAPTER_CONFIG loaded: topicIn={}, statusIn={}, topicOut={}, statusOut={}",
                config.getTopicIn(),
                config.getStatusIn(),
                config.getTopicOut(),
                config.getStatusOut()
        );

        try {
            initProducers(config);
            initConsumers(config);

            running = !producers.isEmpty() || !consumers.isEmpty();

            log.info(
                    "Workers initialized. running={}, producers={}, consumers={}",
                    running,
                    producers.size(),
                    consumers.size()
            );

        } catch (Exception e) {
            log.error("Error starting workers", e);

            stopWorkersInternal();

            throw new IllegalStateException(
                    "Failed to start app-adapter workers",
                    e
            );
        }
    }

    private void initProducers(AppAdapterConfig config) {
        if (!Long.valueOf(1L).equals(config.getStatusOut())) {
            log.warn("Producer disabled by APP_ADAPTER_CONFIG STATUS_OUT={}",
                    config.getStatusOut());
            return;
        }

        if (config.getTopicOut() == null || config.getTopicOut().isBlank()) {
            log.warn("Producer TOPIC_OUT is empty. Producer will not be started");
            return;
        }

        if (producerPoolSize <= 0) {
            log.warn("Producer pool size is {}. Producer will not be started",
                    producerPoolSize);
            return;
        }

        log.info(
                "Initializing {} producer(s) for topic {}",
                producerPoolSize,
                config.getTopicOut()
        );

        AtomicInteger threadCounter = new AtomicInteger();

        producerExecutor = Executors.newFixedThreadPool(
                producerPoolSize,
                runnable -> {
                    Thread thread = new Thread(runnable);

                    thread.setName(
                            "producer-worker-thread-" +
                                    threadCounter.incrementAndGet()
                    );

                    thread.setDaemon(true);

                    return thread;
                }
        );

        for (int i = 0; i < producerPoolSize; i++) {
            GruProducerKafka producer =
                    applicationContext.getBean(GruProducerKafka.class);

            producer.init(
                    config.getTopicOut(),
                    delay,
                    i + 1
            );

            producers.add(producer);
            producerExecutor.submit(producer);
        }
    }

    private void initConsumers(AppAdapterConfig config) {
        if (!Long.valueOf(1L).equals(config.getStatusIn())) {
            log.warn("Consumer disabled by APP_ADAPTER_CONFIG STATUS_IN={}",
                    config.getStatusIn());
            return;
        }

        if (config.getTopicIn() == null || config.getTopicIn().isBlank()) {
            log.warn("Consumer TOPIC_IN is empty. Consumer will not be started");
            return;
        }

        if (consumerPoolSize <= 0) {
            log.warn("Consumer pool size is {}. Consumer will not be started",
                    consumerPoolSize);
            return;
        }

        log.info(
                "Initializing {} consumer(s) for topic {}",
                consumerPoolSize,
                config.getTopicIn()
        );

        for (int i = 0; i < consumerPoolSize; i++) {
            GruConsumerKafka consumer =
                    applicationContext.getBean(GruConsumerKafka.class);

            consumer.start(
                    config.getTopicIn(),
                    i + 1
            );

            consumers.add(consumer);
        }
    }

    private void stopWorkersInternal() {
        log.info(
                "Stopping workers. Producers={}, Consumers={}",
                producers.size(),
                consumers.size()
        );

        for (GruProducerKafka producer : producers) {
            try {
                producer.stop();
            } catch (Exception e) {
                log.error("Error stopping producer", e);
            }
        }

        if (producerExecutor != null) {
            producerExecutor.shutdownNow();

            try {
                if (!producerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Producer executor did not terminate within timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for producer executor shutdown");
            }

            producerExecutor = null;
        }

        for (GruConsumerKafka consumer : consumers) {
            try {
                consumer.stop();
            } catch (Exception e) {
                log.error("Error stopping consumer", e);
            }
        }

        producers.clear();
        consumers.clear();

        running = false;

        log.info("All app-adapter workers stopped");
    }

    public boolean isRunning() {
        return running;
    }

    public int getProducerCount() {
        synchronized (lifecycleMonitor) {
            return producers.size();
        }
    }

    public int getConsumerCount() {
        synchronized (lifecycleMonitor) {
            return consumers.size();
        }
    }

    @PreDestroy
    public void shutdown() {
        synchronized (lifecycleMonitor) {
            log.info("AppInitializer @PreDestroy invoked");
            stopWorkersInternal();
        }
    }
}