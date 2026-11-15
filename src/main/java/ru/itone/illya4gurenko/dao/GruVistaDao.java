package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * слой доступа к бизнес-записям для обработки балансов (таблица GRU_VISTA_TAB).
 */
@Component
@RequiredArgsConstructor
public class GruVistaDao {
    private final GruVistaTabRepository repository;
    private final SqlSessionFactory factory;

    /**
     * выбирает порцию записей в статусе WAIT и блокирует их (FOR UPDATE SKIP LOCKED).
     */
    public List<GruVistaTab> fetchBatchForUpdate(int limit) {
        return repository.fetchBatchForUpdate(limit);
    }

    /**
     * ищет записи по идентификатору пакета svfeLoadId (requestId).
     */
    public List<GruVistaTab> findBySvfeLoadId(String svfeLoadId) {
        if (svfeLoadId == null || svfeLoadId.isBlank()) {
            return Collections.emptyList();
        }
        return repository.findBySvfeLoadId(svfeLoadId);
    }

    /**
     * пакетно обновляет статус обработки и проставляет svfeLoadId для переданного списка идентификаторов.
     */
    public void updateBatchStatusAndSvfeLoadId(List<Long> ids, FocStatus newStatus, String svfeLoadId) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        repository.updateBatchStatusAndSvfeLoadId(ids, newStatus, svfeLoadId);
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
     * пакетно обновляет остатки балансов и финальные статусы (SUCCESS/ERROR) для списка записей за один JDBC-батч.
     */
    @Transactional
    public void finalUpdateGru(List<GruVistaTab> gruVistaTabs) {
        if (gruVistaTabs == null || gruVistaTabs.isEmpty()) {
            return;
        }
        try (SqlSession session = factory.openSession(ExecutorType.BATCH, false)) {
            try {
                GruVistaTabRepository mapper = session.getMapper(GruVistaTabRepository.class);
                for (GruVistaTab vistaTab : gruVistaTabs) {
                    mapper.updateBalanceAndStatus(
                            vistaTab.getId(),
                            vistaTab.getOldTBal(),
                            vistaTab.getNewTBal(),
                            vistaTab.getFocStatus()
                    );
                }
                session.flushStatements();
                session.commit();
            } catch (Exception e) {
                try {
                    session.rollback();
                } catch (Exception rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw new IllegalStateException("Failed to batch update GRU_VISTA_TAB", e);
            }
        }
    }

    /**
     * интеллектуально возвращает брошенные в IN_PROCESS записи обратно в статус WAIT
     * (только если для них НЕТ активной транзакции в APP_ADAPTER_TRANS).
     */
    public int resetOrphanedStuckRecords() {
        return repository.resetOrphanedStuckRecords();
    }
}
