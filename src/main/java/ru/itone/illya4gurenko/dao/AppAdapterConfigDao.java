package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.repository.AppAdapterConfigRepository;

/**
 * слой доступа к конфигурации адаптера (таблица APP_ADAPTER_CONFIG).
 */
@Component
@RequiredArgsConstructor
public class AppAdapterConfigDao {
    private final AppAdapterConfigRepository repository;

    /**
     * загружает конфигурационные параметры по идентификатору системы.
     */
    public AppAdapterConfig getBySystemId(String systemId) {
        return repository.findBySystemId(systemId);
    }
}