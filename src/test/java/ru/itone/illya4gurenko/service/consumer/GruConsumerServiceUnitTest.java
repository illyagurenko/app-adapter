package ru.itone.illya4gurenko.service.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dao.GruRejectDao;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ConsumerEventDto;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.GruValidationResult;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.MapperUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class GruConsumerServiceUnitTest {

    private final MapperUtils mapperUtils = Mockito.mock(MapperUtils.class);
    private final AppAdapterTransDao transDao = Mockito.mock(AppAdapterTransDao.class);
    private final GruVistaDao gruVistaDao = Mockito.mock(GruVistaDao.class);
    private final GruRejectDao gruRejectDao = Mockito.mock(GruRejectDao.class);
    private final TransactionTemplate transactionTemplate = Mockito.mock(TransactionTemplate.class);
    private final GruValidateService gruValidateService = Mockito.mock(GruValidateService.class);
    private final ConsumerStatusService consumerStatusService = Mockito.mock(ConsumerStatusService.class);

    private GruConsumerService consumerService;

    @BeforeEach
    void setUp() {
        consumerService = new GruConsumerService(
                mapperUtils,
                transDao,
                gruVistaDao,
                gruRejectDao,
                transactionTemplate,
                gruValidateService,
                consumerStatusService
        );
        ReflectionTestUtils.setField(consumerService, "numberPC", 161L);

        doAnswer(invocation -> {
            Consumer<TransactionStatus> action = invocation.getArgument(0);
            action.accept(new SimpleTransactionStatus());
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());
    }

    @Test
    @DisplayName("пропуск некорректного json")
    void testConsumeInvalidJson() {
        when(mapperUtils.tryMappingToConsumerKafkaDto("invalid")).thenReturn(null);

        assertDoesNotThrow(() -> consumerService.consume("invalid"));
    }

    @Test
    @DisplayName("пропуск сообщения без requestId")
    void testConsumeMissingRequestId() {
        ConsumerKafkaDto dto = new ConsumerKafkaDto().setRequestId("");
        when(mapperUtils.tryMappingToConsumerKafkaDto("{}")).thenReturn(dto);

        assertDoesNotThrow(() -> consumerService.consume("{}"));
    }

    @Test
    @DisplayName("пропуск уже обработанного сообщения")
    void testConsumeAlreadyProcessed() {
        ConsumerKafkaDto dto = new ConsumerKafkaDto().setRequestId("req-done");
        when(mapperUtils.tryMappingToConsumerKafkaDto("json")).thenReturn(dto);

        AppAdapterTrans trans = new AppAdapterTrans().setStatus(FocStatus.SUCCESS);
        when(transDao.findByRequestId("req-done")).thenReturn(trans);

        consumerService.consume("json");

        verify(consumerStatusService, never()).startProcessing(any(), any(), any());
    }

    @Test
    @DisplayName("успешная обработка сообщения консьюмером")
    void testConsumeNormalSuccess() {
        ConsumerKafkaDto dto = new ConsumerKafkaDto().setRequestId("req-ok").setStatus("SUCCESS");
        when(mapperUtils.tryMappingToConsumerKafkaDto("json-ok")).thenReturn(dto);

        AppAdapterTrans trans = new AppAdapterTrans().setStatus(FocStatus.CONSUMER_WAIT);
        when(transDao.findByRequestId("req-ok")).thenReturn(trans);

        GruVistaTab row = new GruVistaTab().setId(1L);
        when(gruVistaDao.findBySvfeLoadId("req-ok")).thenReturn(List.of(row));

        GruValidationResult valResult = new GruValidationResult(List.of(row), Collections.emptyList(), false, null);
        when(gruValidateService.validate(dto, List.of(row))).thenReturn(valResult);

        consumerService.consume("json-ok");

        verify(consumerStatusService).updateFinalStatus(trans, FocStatus.SUCCESS, "0", "SUCCESS");
    }

    @Test
    @DisplayName("обработка сообщения при отсутствии транзакции в базе")
    void testConsumeTransNotFoundDirectValidation() {
        ConsumerKafkaDto dto = new ConsumerKafkaDto().setRequestId("req-orphan").setStatus("SUCCESS");
        when(mapperUtils.tryMappingToConsumerKafkaDto("json-orphan")).thenReturn(dto);
        when(transDao.findByRequestId("req-orphan")).thenReturn(null);
        when(gruVistaDao.findBySvfeLoadId("req-orphan")).thenReturn(Collections.emptyList());

        assertDoesNotThrow(() -> consumerService.consume("json-orphan"));
    }
}

