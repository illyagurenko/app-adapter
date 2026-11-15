package ru.itone.illya4gurenko.dao;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.controller.dto.ConsumerMetricsResponse;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.repository.AppAdapterTransRepository;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AppAdapterTransDaoTest {

    private final AppAdapterTransRepository repository = Mockito.mock(AppAdapterTransRepository.class);
    private final AppAdapterTransDao dao = new AppAdapterTransDao(repository);

    @Test
    @DisplayName("сохранение транзакции адаптера")
    void testSave() {
        AppAdapterTrans trans = new AppAdapterTrans();
        dao.save(trans);
        verify(repository).save(trans);
    }

    @Test
    @DisplayName("обновление статуса транзакции адаптера")
    void testUpdateStatus() {
        AppAdapterTrans trans = new AppAdapterTrans();
        dao.updateStatus(trans);
        verify(repository).updateStatus(trans);
    }

    @Test
    @DisplayName("поиск транзакции по requestId")
    void testFindByRequestId() {
        AppAdapterTrans trans = new AppAdapterTrans().setRequestId("r1");
        when(repository.findByRequestId("r1")).thenReturn(trans);

        AppAdapterTrans result = dao.findByRequestId("r1");

        assertEquals("r1", result.getRequestId());
    }

    @Test
    @DisplayName("получение метрик работы консьюмера")
    void testGetMetrics() {
        ConsumerMetricsResponse response = new ConsumerMetricsResponse(1, null, null, 1.0, "1 sec", 1.0, 1.0, 1.0, 100);
        when(repository.calculateMetrics()).thenReturn(response);

        ConsumerMetricsResponse result = dao.getMetrics();

        assertEquals(1, result.totalCompleted());
    }
}

