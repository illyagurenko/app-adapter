package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.math.BigDecimal;
import java.util.List;

/**
 * слой доступа к бизнес-записям для обработки балансов (таблица GRU_VISTA_TAB).
 */
@Component
@RequiredArgsConstructor
public class GruVistaDao {
    private final GruVistaTabRepository repository;

    /**
     * выбирает порцию записей в статусе WAIT и блокирует их от других потоков (FOR UPDATE SKIP LOCKED).
     */
    public List<GruVistaTab> fetchBatchForUpdate(int limit) {
        return repository.fetchBatchForUpdate(limit);
    }

    /**
     * пакетно обновляет статус обработки для переданного списка идентификаторов записей.
     */
    public void updateStatusByIds(List<Long> ids, FocStatus newStatus) {
        // защита от вызова с пустым списком, чтобы не ломать sql-запрос IN ()
        if (ids == null || ids.isEmpty()) {
            return;
        }
        repository.updateStatusByIds(ids, newStatus);
    }

    /**
     * обновляет остатки балансов (старый и новый) и выставляет финальный статус записи.
     */
    public void updateBalanceAndStatus(Long id, BigDecimal oldTBal, BigDecimal newTBal, FocStatus status) {
        repository.updateBalanceAndStatus(id, oldTBal, newTBal, status);
    }

    /**
     * сбрасывает зависшие в IN_PROCESS записи обратно в WAIT, если прошло больше указанных минут.
     */
    public void resetStuckRecords(int minutes) {
        repository.resetStuckRecords(minutes);
    }
}