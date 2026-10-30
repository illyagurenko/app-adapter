package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import ru.itone.illya4gurenko.entity.GruRejectTab;

import java.util.List;

/**
 * mybatis-репозиторий для сохранения забракованных записей (GRU_REJECT_TAB).
 */
@Mapper
public interface GruRejectTabRepository {

    /**
     * сохраняет запись с описанием ошибки и брака.
     */
    void save(GruRejectTab rejectTab);

    /**
     * возвращает все записи забракованных транзакций.
     */
    List<GruRejectTab> findAll();
}