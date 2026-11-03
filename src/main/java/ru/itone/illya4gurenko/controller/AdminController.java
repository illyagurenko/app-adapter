package ru.itone.illya4gurenko.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.itone.illya4gurenko.controller.dto.AdminResponse;
import ru.itone.illya4gurenko.controller.dto.ConsumerMetricsResponse;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.config.AppInitializer;

/**
 * контроллер для ручного администрирования фоновых процессов и мониторинга скорости адаптера.
 */
@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
@Slf4j
public class AdminController {

    private final AppInitializer appInitializer;
    private final AppAdapterTransDao transDao;

    /**
     * возвращает текущее состояние worker-потоков сервиса.
     */
    @GetMapping("/status")
    public ResponseEntity<AdminResponse> status() {
        return ResponseEntity.ok(
                buildResponse("Current app-adapter state")
        );
    }

    /**
     * возвращает агрегированные метрики времени обработки транзакций консьюмером.
     */
    @GetMapping("/metrics")
    public ResponseEntity<ConsumerMetricsResponse> metrics() {
        ConsumerMetricsResponse raw = transDao.getMetrics();

        if (raw == null || raw.totalCompleted() == 0) {
            return ResponseEntity.ok(new ConsumerMetricsResponse(
                    0, null, null, 0.0, "0 secs", 0.0, 0.0, 0.0, 0
            ));
        }

        double totalSec = raw.totalDurationSeconds();
        long minutes = (long) (totalSec / 60);
        double remainingSec = totalSec % 60;
        String formatted = minutes > 0
                ? String.format("%d mins %.2f secs", minutes, remainingSec)
                : String.format("%.2f secs", remainingSec);

        double batchesPerSec = totalSec > 0 ? (raw.totalCompleted() / totalSec) : 0;
        long estimatedRows = raw.totalCompleted() * 100L;

        ConsumerMetricsResponse enriched = new ConsumerMetricsResponse(
                raw.totalCompleted(),
                raw.firstStartTime(),
                raw.lastFinishTime(),
                totalSec,
                formatted,
                raw.avgDurationSeconds(),
                raw.maxDurationSeconds(),
                Math.round(batchesPerSec * 100.0) / 100.0,
                estimatedRows
        );

        return ResponseEntity.ok(enriched);
    }

    /**
     * перезачитывает таблицу APP_ADAPTER_CONFIG и перезапускает все потоки.
     */
    @PostMapping("/restart")
    public ResponseEntity<AdminResponse> restart() {
        log.warn("ADMIN: restart workers requested");

        appInitializer.restartWorkers();

        return ResponseEntity.ok(
                buildResponse("APP_ADAPTER_CONFIG reloaded and workers restarted")
        );
    }

    /**
     * останавливает фоновые потоки отправки и чтения, сохраняя работу rest-сервиса.
     */
    @PostMapping("/workers/stop")
    public ResponseEntity<AdminResponse> stopWorkers() {
        log.warn("ADMIN: stop workers requested");

        appInitializer.stopWorkers();

        return ResponseEntity.ok(
                buildResponse("All workers stopped")
        );
    }

    /**
     * инициирует корректное выключение приложения и завершение jvm.
     */
    @PostMapping("/shutdown")
    public ResponseEntity<AdminResponse> shutdown() {
        log.warn("ADMIN: full application shutdown requested");

        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            log.warn("ADMIN: shutting down JVM");

            System.exit(0);
        });

        shutdownThread.setName("admin-shutdown-thread");
        shutdownThread.setDaemon(false);
        shutdownThread.start();

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(buildResponse("Application shutdown initiated"));
    }

    /**
     * формирует объект ответа с текущим статусом сервиса.
     */
    private AdminResponse buildResponse(String message) {
        return new AdminResponse(
                message,
                appInitializer.isRunning(),
                appInitializer.getProducerCount(),
                appInitializer.getConsumerCount()
        );
    }
}