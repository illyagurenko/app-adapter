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
import ru.itone.illya4gurenko.сonfig.AppInitializer;

@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
@Slf4j
public class AdminController {

    private final AppInitializer appInitializer;

    /**
     * текущее состояние worker-потоков.
     */
    @GetMapping("/status")
    public ResponseEntity<AdminResponse> status() {
        return ResponseEntity.ok(
                buildResponse("Current app-adapter state")
        );
    }

    /**
     * перечитать APP_ADAPTER_CONFIG и пересоздать все workers.
     */
    @PostMapping("/restart")
    public ResponseEntity<AdminResponse> restart() {
        log.warn("ADMIN: restart workers requested");

        appInitializer.restartWorkers();

        return ResponseEntity.ok(
                buildResponse(
                        "APP_ADAPTER_CONFIG reloaded and workers restarted"
                )
        );
    }

    /**
     * остановить producer и consumer.
     * REST-сервис продолжает работать.
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
     * Полное завершение JVM
     * Ответ HTTP сначала возвращается клиенту,
     * затем System.exit(0) инициирует shutdown
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
                .body(
                        buildResponse(
                                "Application shutdown initiated"
                        )
                );
    }

    private AdminResponse buildResponse(String message) {
        return new AdminResponse(
                message,
                appInitializer.isRunning(),
                appInitializer.getProducerCount(),
                appInitializer.getConsumerCount()
        );
    }


}