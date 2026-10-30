package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;

import java.util.List;

/**
 * mybatis-репозиторий для таблицы конфигурации адаптера (APP_ADAPTER_CONFIG).
 */
@Mapper
public interface AppAdapterConfigRepository {

    /**
     * сохраняет новую запись конфигурации адаптера.
     */
    void save(AppAdapterConfig config);

    /**
     * ищет конфигурацию адаптера по идентификатору системы (systemId).
     */
    AppAdapterConfig findBySystemId(@Param("systemId") String systemId);

    /**
     * возвращает список всех существующих конфигураций.
     */
    List<AppAdapterConfig> findAll();

}