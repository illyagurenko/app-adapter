package ru.itone.illya4gurenko.monitor;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * сервис периодического мониторинга заполненности буфера kafka producer через micrometer.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaBufferMonitor {

    private final MeterRegistry meterRegistry;

    /**
     * проверяет размер доступного буфера продюсера кафки и предупреждает при переполнении > 80%.
     */
    @Scheduled(fixedRate = 30000)
    public void monitorProducerBuffer() {
        try {
            // метрика доступного места в буфере
            double availableBytes = meterRegistry.get("kafka.producer.buffer.available.bytes")
                    .gauge()
                    .value();

            // общий размер буфера
            double totalBytes = meterRegistry.get("kafka.producer.buffer.total.bytes")
                    .gauge()
                    .value();

            double usedBytes = totalBytes - availableBytes;
            double usedPercentage = (usedBytes / totalBytes) * 100;

            double usedMb = usedBytes / (1024 * 1024);
            double totalMb = totalBytes / (1024 * 1024);

            if (usedPercentage > 80.0) {
                log.warn("kafka buffer warning: Used {} MB of {} MB ({}%)",
                        String.format("%.2f", usedMb),
                        String.format("%.2f", totalMb),
                        String.format("%.2f", usedPercentage));
            } else {
                log.info("kafka producer buffer: {} MB / {} MB used ({}%)",
                        String.format("%.2f", usedMb),
                        String.format("%.2f", totalMb),
                        String.format("%.1f", usedPercentage));
            }

        } catch (Exception e) {
            log.trace("Kafka metrics not available yet");
        }
    }
}