package ru.itone.illya4gurenko.service.recovery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.GruVistaDao;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StuckRecordsRecoveryServiceTest {

    private final GruVistaDao gruVistaDao = Mockito.mock(GruVistaDao.class);
    private final StuckRecordsRecoveryService recoveryService = new StuckRecordsRecoveryService(gruVistaDao);

    @Test
    @DisplayName("восстановление зависших записей в базе")
    void testRecoverStuckRecords() {
        when(gruVistaDao.resetOrphanedStuckRecords()).thenReturn(5);

        recoveryService.recoverStuckRecords();

        verify(gruVistaDao).resetOrphanedStuckRecords();
    }

    @Test
    @DisplayName("обработка исключения при восстановлении записей")
    void testRecoverStuckRecordsHandlesException() {
        when(gruVistaDao.resetOrphanedStuckRecords()).thenThrow(new RuntimeException("DB error"));

        assertDoesNotThrow(() -> recoveryService.recoverStuckRecords());
    }
}

