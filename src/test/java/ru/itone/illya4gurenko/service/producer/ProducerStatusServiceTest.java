package ru.itone.illya4gurenko.service.producer;

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

class ProducerStatusServiceTest {

    private final AppAdapterTransDao transDao = Mockito.mock(AppAdapterTransDao.class);
    private final AppAdapterIoMsgsDao ioMsgsDao = Mockito.mock(AppAdapterIoMsgsDao.class);
    private final AdapterEntityFactory adapterEntityFactory = new AdapterEntityFactory();
    private final ProducerStatusService statusService = new ProducerStatusService(transDao, ioMsgsDao, adapterEntityFactory);

    @Test
    @DisplayName("сохранение начальной транзакции продюсером")
    void testSaveInitialTrans() {
        AppAdapterTrans trans = new AppAdapterTrans().setRequestId("req-p");

        statusService.saveInitialTrans(trans);

        assertAll(
                () -> assertEquals(FocStatus.CONSUMER_WAIT, trans.getStatus()),
                () -> assertEquals("Waiting for Kafka response", trans.getRespDesc()),
                () -> verify(transDao).save(trans)
        );
    }

    @Test
    @DisplayName("сохранение успешного лога отправки в io_msgs")
    void testSaveSuccessLog() {
        AppAdapterTrans trans = new AppAdapterTrans().setId(5L);

        statusService.saveSuccessLog(trans, "{}", 161L);

        verify(ioMsgsDao).save(any());
    }

    @Test
    @DisplayName("пометка транзакции как ошибочной при сбое отправки")
    void testMarkAsError() {
        AppAdapterTrans trans = new AppAdapterTrans().setId(6L);

        statusService.markAsError(trans, "Kafka timeout");

        assertAll(
                () -> assertEquals(FocStatus.ERROR, trans.getStatus()),
                () -> assertEquals("500", trans.getRespCode()),
                () -> assertEquals("Kafka timeout", trans.getRespDesc()),
                () -> verify(transDao).updateStatus(trans)
        );
    }
}

