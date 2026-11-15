package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;

import java.math.BigDecimal;
import java.util.List;

/**
 * mybatis-репозиторий для работы с бизнес-данными балансов (GRU_VISTA_TAB).
 */
@Mapper
public interface GruVistaTabRepository {

    /**
     * сохраняет новую запись баланса.
     */
    void save(GruVistaTab vistaTab);

    /**
     * возвращает все записи балансов.
     */
    List<GruVistaTab> findAll();

    /**
     * блокирует и возвращает порцию записей со статусом WAIT с учетом расписания INTIME/IMMEDIATE.
     */
    List<GruVistaTab> fetchBatchForUpdate(@Param("limit") int limit);

    /**
     * ищет записи по идентификатору пакета svfeLoadId (requestId).
     */
    List<GruVistaTab> findBySvfeLoadId(@Param("svfeLoadId") String svfeLoadId);

    /**
     * массово переводит записи по их id в новый статус и проставляет svfeLoadId.
     */
    void updateBatchStatusAndSvfeLoadId(@Param("ids") List<Long> ids,
                                        @Param("newStatus") FocStatus newStatus,
                                        @Param("svfeLoadId") String svfeLoadId);

    /**
     * массово переводит записи по их id в новый статус.
     */
    void updateStatusByIds(@Param("ids") List<Long> ids, @Param("newStatus") FocStatus newStatus);

    /**
     * обновляет остатки и устанавливает финальный статус (SUCCESS/ERROR) для конкретной записи.
     */
    void updateBalanceAndStatus(@Param("id") Long id,
                                @Param("oldTBal") BigDecimal oldTBal,
                                @Param("newTBal") BigDecimal newTBal,
                                @Param("status") FocStatus status);

    /**
     * интеллектуально возвращает брошенные в IN_PROCESS записи обратно в статус WAIT
     * (только если для них НЕТ активной транзакции в APP_ADAPTER_TRANS).
     */
    int resetOrphanedStuckRecords();
}