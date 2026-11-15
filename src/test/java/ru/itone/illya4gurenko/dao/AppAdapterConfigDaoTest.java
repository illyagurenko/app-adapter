package ru.itone.illya4gurenko.dao;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.entity.AppAdapterConfig;
import ru.itone.illya4gurenko.entity.enums.MsgType;
import ru.itone.illya4gurenko.repository.AppAdapterConfigRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AppAdapterConfigDaoTest {

    private final AppAdapterConfigRepository repository = Mockito.mock(AppAdapterConfigRepository.class);
    private final AppAdapterConfigDao dao = new AppAdapterConfigDao(repository);

    @Test
    @DisplayName("получение конфигурации по идентификатору системы")
    void testGetBySystemId() {
        AppAdapterConfig config = new AppAdapterConfig().setSystemId(MsgType.GRU);
        when(repository.findBySystemId("GRU")).thenReturn(config);

        AppAdapterConfig result = dao.getBySystemId("GRU");

        assertEquals(MsgType.GRU, result.getSystemId());
    }
}

