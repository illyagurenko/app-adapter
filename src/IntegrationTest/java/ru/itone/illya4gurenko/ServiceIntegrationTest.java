package ru.itone.illya4gurenko;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import ru.itone.illya4gurenko.config.AppInitializer;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dao.GruRejectDao;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.Type;
import ru.itone.illya4gurenko.service.consumer.GruConsumerService;
import ru.itone.illya4gurenko.service.producer.GruProducerService;
import ru.itone.illya4gurenko.service.producer.KafkaProducerSender;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

@SpringBootTest
class ServiceIntegrationTest {

    @MockBean
    private AppInitializer appInitializer;

    @MockBean
    private AppAdapterTransDao transDao;

    @MockBean
    private GruVistaDao gruVistaDao;

    @MockBean
    private GruRejectDao gruRejectDao;

    @MockBean
    private AppAdapterIoMsgsDao ioMsgsDao;

    @MockBean
    private KafkaProducerSender kafkaProducerSender;

    @Autowired
    private GruConsumerService gruConsumerService;

    @Autowired
    private GruProducerService gruProducerService;

    @Test
    @DisplayName("успешная обработка ответа консьюмером")
    void testConsumerSuccess() {
        AppAdapterTrans trans = new AppAdapterTrans()
                .setRequestId("req-success")
                .setStatus(FocStatus.CONSUMER_WAIT);
        Mockito.when(transDao.findByRequestId("req-success")).thenReturn(trans);

        GruVistaTab row = new GruVistaTab().setId(1L).setSystemAccount("ACC_1");
        Mockito.when(gruVistaDao.findBySvfeLoadId("req-success")).thenReturn(List.of(row));

        String successJson = """
                {
                  "actualTimestamp": 1000000,
                  "systemId": "SMART",
                  "requestId": "req-success",
                  "eventType": "BALANCE",
                  "entityType": "ACCOUNT",
                  "status": "SUCCESS",
                  "events": [
                    {
                      "entityValue": "ACC_1",
                      "entityId": 1,
                      "status": "SUCCESS",
                      "data": { "oldTbal": 100, "newTbal": 200 }
                    }
                  ]
                }
                """;

        gruConsumerService.consume(successJson);

        assertEquals(FocStatus.SUCCESS, trans.getStatus());
    }

    @Test
    @DisplayName("обработка ошибочного ответа консьюмером")
    void testConsumerError() {
        AppAdapterTrans trans = new AppAdapterTrans()
                .setRequestId("req-error")
                .setStatus(FocStatus.CONSUMER_WAIT);
        Mockito.when(transDao.findByRequestId("req-error")).thenReturn(trans);

        GruVistaTab row = new GruVistaTab().setId(2L).setSystemAccount("ACC_2");
        Mockito.when(gruVistaDao.findBySvfeLoadId("req-error")).thenReturn(List.of(row));

        String errorJson = """
                {
                  "actualTimestamp": 1000000,
                  "systemId": "SMART",
                  "requestId": "req-error",
                  "eventType": "BALANCE",
                  "entityType": "ACCOUNT",
                  "status": "ERROR",
                  "error": { "code": "500", "message": "General batch error" }
                }
                """;

        gruConsumerService.consume(errorJson);

        assertEquals(FocStatus.ERROR, trans.getStatus());
    }

    @Test
    @DisplayName("успешная отправка пакета продюсером")
    void testProducerSuccess() {
        GruVistaTab row = new GruVistaTab()
                .setId(10L)
                .setSystemAccount("ACC_PROD")
                .setCurrency("RUB")
                .setXalfa(BigDecimal.ONE)
                .setOperation(Type.DR);
        Mockito.when(gruVistaDao.fetchBatchForUpdate(anyInt())).thenReturn(List.of(row));

        boolean result = gruProducerService.produce("topic-out");

        assertTrue(result);
    }

    @Test
    @DisplayName("обработка ошибки отправки продюсером")
    void testProducerError() {
        GruVistaTab row = new GruVistaTab()
                .setId(20L)
                .setSystemAccount("ACC_ERR")
                .setCurrency("RUB")
                .setXalfa(BigDecimal.ONE)
                .setOperation(Type.DR);
        Mockito.when(gruVistaDao.fetchBatchForUpdate(anyInt())).thenReturn(List.of(row));
        Mockito.doThrow(new RuntimeException("Kafka unavailable"))
                .when(kafkaProducerSender).send(any(), any(), any());

        boolean result = gruProducerService.produce("topic-out");

        assertFalse(result);
    }
}
