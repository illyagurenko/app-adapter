package ru.itone.illya4gurenko.service.recovery;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dao.GruVistaDao;

/**
 * регламентный сервис восстановления брошенных записей, оставшихся в статусе IN_PROCESS
 * при падениях JVM, обрывах сети или нештатных сбоях продюсера.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StuckRecordsRecoveryService {

    private final GruVistaDao gruVistaDao;

    /**
     * периодически сбрасывает статус брошенных записей из IN_PROCESS обратно в WAIT
     * (только если для них НЕТ активной транзакции в APP_ADAPTER_TRANS).
     * Безопасен для любого объема очереди (хоть 10 млн записей).
     */
    @Scheduled(fixedDelayString = "${recovery.stuck-records.delay:60000}", initialDelay = 15000)
    public void recoverStuckRecords() {
        try {
            int resetCount = gruVistaDao.resetOrphanedStuckRecords();
            if (resetCount > 0) {
                log.warn("Recovery job reset {} orphaned IN_PROCESS records back to WAIT", resetCount);
            }
        } catch (Exception e) {
            log.error("Error during orphaned records recovery job", e);
        }
    }
}
