package ru.itone.illya4gurenko.service.producer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GruBatchingServiceTest {

    private final GruVistaDao gruVistaDao = Mockito.mock(GruVistaDao.class);
    private final AdapterEntityFactory factory = new AdapterEntityFactory();
    private final GruBatchingService batchingService = new GruBatchingService(gruVistaDao, factory);

    @Test
    @DisplayName("батчинг при пустом списке записей")
    void testBatchAndUpdateEmpty() {
        when(gruVistaDao.fetchBatchForUpdate(100)).thenReturn(Collections.emptyList());

        List<ProducerKafkaDto> dtos = batchingService.batchAndUpdate(100, 10);

        assertTrue(dtos.isEmpty());
    }

    @Test
    @DisplayName("разбиение записей на чанки и обновление статусов")
    void testBatchAndUpdateChunks() {
        GruVistaTab r1 = new GruVistaTab().setId(1L).setSystemAccount("A1");
        GruVistaTab r2 = new GruVistaTab().setId(2L).setSystemAccount("A2");
        GruVistaTab r3 = new GruVistaTab().setId(3L).setSystemAccount("A3");

        when(gruVistaDao.fetchBatchForUpdate(100)).thenReturn(List.of(r1, r2, r3));

        List<ProducerKafkaDto> dtos = batchingService.batchAndUpdate(100, 2);

        assertAll(
                () -> assertEquals(2, dtos.size()),
                () -> assertEquals(2, dtos.get(0).getEvents().size()),
                () -> assertEquals(1, dtos.get(1).getEvents().size()),
                () -> verify(gruVistaDao, Mockito.times(2))
                        .updateBatchStatusAndSvfeLoadId(any(), eq(FocStatus.IN_PROCESS), any())
        );
    }
}

