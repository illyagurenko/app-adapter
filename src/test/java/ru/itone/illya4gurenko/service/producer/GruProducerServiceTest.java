package ru.itone.illya4gurenko.service.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ProducerEventDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;
import ru.itone.illya4gurenko.utils.MapperUtils;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class GruProducerServiceTest {

    private final GruBatchingService batchingService = Mockito.mock(GruBatchingService.class);
    private final MapperUtils mapperUtils = Mockito.mock(MapperUtils.class);
    private final AdapterEntityFactory adapterEntityFactory = new AdapterEntityFactory();
    private final KafkaProducerSender kafkaProducerSender = Mockito.mock(KafkaProducerSender.class);
    private final ProducerStatusService producerStatusService = Mockito.mock(ProducerStatusService.class);
    private final GruVistaDao gruVistaDao = Mockito.mock(GruVistaDao.class);

    private GruProducerService producerService;

    @BeforeEach
    void setUp() {
        producerService = new GruProducerService(
                batchingService,
                mapperUtils,
                adapterEntityFactory,
                kafkaProducerSender,
                producerStatusService,
                gruVistaDao
        );
        ReflectionTestUtils.setField(producerService, "fetchSize", 100);
        ReflectionTestUtils.setField(producerService, "batchSize", 10);
        ReflectionTestUtils.setField(producerService, "nodeId", 161L);
    }

    @Test
    @DisplayName("пропуск отправки при пустом списке пакетов")
    void testProduceEmptyList() {
        when(batchingService.batchAndUpdate(100, 10)).thenReturn(Collections.emptyList());

        boolean result = producerService.produce("topic-out");

        assertFalse(result);
    }

    @Test
    @DisplayName("успешная отправка сформированных пакетов продюсером")
    void testProduceSuccessFlow() {
        ProducerEventDto event = new ProducerEventDto().setId(1L);
        ProducerKafkaDto dto = new ProducerKafkaDto().setRequestId("r1").setEvents(List.of(event));

        when(batchingService.batchAndUpdate(100, 10)).thenReturn(List.of(dto));
        when(mapperUtils.tryMappingToStr(dto)).thenReturn("{\"req\":\"r1\"}");

        boolean result = producerService.produce("topic-out");

        assertTrue(result);
    }

    @Test
    @DisplayName("обработка ошибки при отправке в kafka продюсером")
    void testProduceKafkaSendError() {
        ProducerEventDto event = new ProducerEventDto().setId(1L);
        ProducerKafkaDto dto = new ProducerKafkaDto().setRequestId("r1").setEvents(List.of(event));

        when(batchingService.batchAndUpdate(100, 10)).thenReturn(List.of(dto));
        when(mapperUtils.tryMappingToStr(dto)).thenReturn("{\"req\":\"r1\"}");
        doThrow(new RuntimeException("Kafka error")).when(kafkaProducerSender).send(any(), any(), any());

        boolean result = producerService.produce("topic-out");

        assertFalse(result);
    }
}

