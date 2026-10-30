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
     * возвращает зависшие в IN_PROCESS записи обратно в статус WAIT, если истек тайм-аут в минутах.
     */
    void resetStuckRecords(@Param("minutes") int minutes);
}