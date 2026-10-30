package ru.itone.illya4gurenko.service.recovery;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dao.GruVistaDao;

/**
 * регламентный сервис восстановления зависших записей, оставшихся в статусе IN_PROCESS при падениях.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StuckRecordsRecoveryService {

    private final GruVistaDao gruVistaDao;

    /**
     * периодически сбрасывает статус записей из IN_PROCESS обратно в WAIT, если обработка зависла дольше 30 минут.
     */
    @Scheduled(fixedDelay = 900_000)
    public void recoverStuckRecords() {
        log.info("Starting recovery job for stuck IN_PROCESS records...");
        try {
            gruVistaDao.resetStuckRecords(30);
            log.info("Recovery job finished successfully.");
        } catch (Exception e) {
            log.error("Error during recovery job", e);
        }
    }
}