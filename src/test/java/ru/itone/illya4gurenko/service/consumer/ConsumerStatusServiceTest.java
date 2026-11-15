package ru.itone.illya4gurenko.service.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

class ConsumerStatusServiceTest {

    private final AppAdapterTransDao transDao = Mockito.mock(AppAdapterTransDao.class);
    private final AppAdapterIoMsgsDao ioMsgsDao = Mockito.mock(AppAdapterIoMsgsDao.class);
    private final AdapterEntityFactory adapterEntityFactory = new AdapterEntityFactory();
    private final ConsumerStatusService statusService = new ConsumerStatusService(transDao, ioMsgsDao, adapterEntityFactory);

    @Test
    @DisplayName("перевод транзакции в статус обработки консьюмером")
    void testStartProcessing() {
        AppAdapterTrans trans = new AppAdapterTrans().setId(1L).setRequestId("req-1");

        statusService.startProcessing(trans, "{}", 161L);

        assertAll(
                () -> assertEquals(FocStatus.CONSUMER_IN_PROCESS, trans.getStatus()),
                () -> verify(transDao).updateStatus(trans),
                () -> verify(ioMsgsDao).save(any())
        );
    }

    @Test
    @DisplayName("обновление финального статуса транзакции")
    void testUpdateFinalStatus() {
        AppAdapterTrans trans = new AppAdapterTrans().setId(2L).setRequestId("req-2");

        statusService.updateFinalStatus(trans, FocStatus.SUCCESS, "0", "OK");

        assertAll(
                () -> assertEquals(FocStatus.SUCCESS, trans.getStatus()),
                () -> assertEquals("0", trans.getRespCode()),
                () -> assertEquals("OK", trans.getRespDesc()),
                () -> verify(transDao).updateStatus(trans)
        );
    }
}

