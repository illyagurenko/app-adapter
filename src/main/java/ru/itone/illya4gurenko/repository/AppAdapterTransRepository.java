package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;

import java.util.List;

/**
 * mybatis-репозиторий для учета транзакций обработки пакетов (APP_ADAPTER_TRANS).
 */
@Mapper
public interface AppAdapterTransRepository {

    /**
     * фиксирует создание новой транзакции при отправке пакета данных.
     */
    void save(AppAdapterTrans trans);

    /**
     * возвращает список всех транзакций адаптера.
     */
    List<AppAdapterTrans> findAll();

    /**
     * обновляет статус, код и описание результата обработки транзакции.
     */
    void updateStatus(AppAdapterTrans trans);

    /**
     * выполняет поиск транзакции по уникальному идентификатору requestId.
     */
    AppAdapterTrans findByRequestId(@Param("requestId") String requestId);
}