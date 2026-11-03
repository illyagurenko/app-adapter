package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.controller.dto.ConsumerMetricsResponse;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;

/**
 * слой доступа к транзакционному журналу батчей (таблица APP_ADAPTER_TRANS).
 */
@Component
@RequiredArgsConstructor
public class AppAdapterTransDao {
    private final AppAdapterTransRepository repository;

    /**
     * сохраняет запись о сформированном пакете запроса.
     */
    public void save(AppAdapterTrans trans) {
        repository.save(trans);
    }

    /**
     * обновляет текущий статус, код и описание результата обработки транзакции.
     */
    public void updateStatus(AppAdapterTrans trans) {
        repository.updateStatus(trans);
    }

    /**
     * выполняет поиск записи транзакции по уникальному идентификатору запроса requestId.
     */
    public AppAdapterTrans findByRequestId(String requestId) {
        return repository.findByRequestId(requestId);
    }

    /**
     * запрашивает агрегированные метрики времени выполнения из базы.
     */
    public ConsumerMetricsResponse getMetrics() {
        return repository.calculateMetrics();
    }

}