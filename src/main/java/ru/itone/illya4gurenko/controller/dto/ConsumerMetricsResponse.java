package ru.itone.illya4gurenko.controller.dto;
import java.time.LocalDateTime;

/**
 * dto с метриками скорости и времени обработки транзакций консьюмером.
 */
public record ConsumerMetricsResponse(
        long totalCompleted,
        LocalDateTime firstStartTime,
        LocalDateTime lastFinishTime,
        double totalDurationSeconds,
        String formattedDuration,
        double avgDurationSeconds,
        double maxDurationSeconds,
        double batchesPerSecond,
        long estimatedRowsProcessed
) {
}
