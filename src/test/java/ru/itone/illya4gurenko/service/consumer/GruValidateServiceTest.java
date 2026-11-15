package ru.itone.illya4gurenko.service.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.dto.ConsumerEventDto;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ErrorDto;
import ru.itone.illya4gurenko.dto.EventDataDto;
import ru.itone.illya4gurenko.dto.GruValidationResult;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GruValidateServiceTest {

    private final AdapterEntityFactory adapterEntityFactory = new AdapterEntityFactory();
    private final GruValidateService validateService = new GruValidateService(adapterEntityFactory);

    @Test
    @DisplayName("валидация пустых входных данных")
    void testValidateEmptyInputs() {
        GruValidationResult result = validateService.validate(null, Collections.emptyList());
        assertTrue(result.rowsToUpdate().isEmpty());
    }

    @Test
    @DisplayName("валидация ошибки статуса пакета")
    void testValidateBatchStatusError() {
        ConsumerKafkaDto dto = new ConsumerKafkaDto()
                .setRequestId("req-err")
                .setStatus("ERROR")
                .setError(new ErrorDto().setMessage("Batch failed"));

        GruVistaTab row = new GruVistaTab().setId(1L).setSystemAccount("ACC1");
        GruValidationResult result = validateService.validate(dto, List.of(row));

        assertAll(
                () -> assertTrue(result.batchFailed()),
                () -> assertEquals("Batch failed", result.errorReason()),
                () -> assertEquals(1, result.rowsToReject().size())
        );
    }

    @Test
    @DisplayName("успешная валидация пакета событий")
    void testValidateSuccessFlow() {
        GruVistaTab row = new GruVistaTab().setId(10L).setSystemAccount("ACC10");

        ConsumerEventDto event = new ConsumerEventDto()
                .setEntityId(10L)
                .setEntityValue("ACC10")
                .setStatus(FocStatus.SUCCESS)
                .setData(new EventDataDto().setOldTbal(BigDecimal.ONE).setNewTbal(BigDecimal.TEN));

        ConsumerKafkaDto dto = new ConsumerKafkaDto()
                .setRequestId("req-success")
                .setStatus("SUCCESS")
                .setEvents(List.of(event));

        GruValidationResult result = validateService.validate(dto, List.of(row));

        assertAll(
                () -> assertFalse(result.batchFailed()),
                () -> assertEquals(1, result.rowsToUpdate().size()),
                () -> assertEquals(FocStatus.SUCCESS, result.rowsToUpdate().get(0).getFocStatus()),
                () -> assertTrue(result.rowsToReject().isEmpty())
        );
    }

    @Test
    @DisplayName("валидация отдельного события с ошибкой")
    void testValidateEventWithIndividualError() {
        GruVistaTab row = new GruVistaTab().setId(20L).setSystemAccount("ACC20");

        ConsumerEventDto event = new ConsumerEventDto()
                .setEntityId(20L)
                .setEntityValue("ACC20")
                .setStatus(FocStatus.ERROR)
                .setError(new ErrorDto().setCode("400").setMessage("Account error"));

        ConsumerKafkaDto dto = new ConsumerKafkaDto()
                .setRequestId("req-indiv-err")
                .setStatus("SUCCESS")
                .setEvents(List.of(event));

        GruValidationResult result = validateService.validate(dto, List.of(row));

        assertAll(
                () -> assertFalse(result.batchFailed()),
                () -> assertEquals(1, result.rowsToUpdate().size()),
                () -> assertEquals(FocStatus.ERROR, result.rowsToUpdate().get(0).getFocStatus()),
                () -> assertEquals(1, result.rowsToReject().size())
        );
    }

    @Test
    @DisplayName("валидация неполного набора строк в ответе")
    void testValidateMissingRowsInResponse() {
        GruVistaTab row1 = new GruVistaTab().setId(1L).setSystemAccount("A1");
        GruVistaTab row2 = new GruVistaTab().setId(2L).setSystemAccount("A2");

        ConsumerEventDto event1 = new ConsumerEventDto()
                .setEntityId(1L)
                .setEntityValue("A1")
                .setStatus(FocStatus.SUCCESS);

        ConsumerKafkaDto dto = new ConsumerKafkaDto()
                .setRequestId("req-incomplete")
                .setStatus("SUCCESS")
                .setEvents(List.of(event1));

        GruValidationResult result = validateService.validate(dto, List.of(row1, row2));

        assertAll(
                () -> assertTrue(result.batchFailed()),
                () -> assertEquals("Not all rows returned in response", result.errorReason()),
                () -> assertEquals(2, result.rowsToReject().size())
        );
    }

    @Test
    @DisplayName("валидация неизвестного entityId")
    void testValidateDuplicateOrUnknownEntityId() {
        GruVistaTab row1 = new GruVistaTab().setId(1L).setSystemAccount("A1");

        ConsumerEventDto event1 = new ConsumerEventDto()
                .setEntityId(1L)
                .setStatus(FocStatus.SUCCESS);
        ConsumerEventDto unknownEvent = new ConsumerEventDto()
                .setEntityId(999L)
                .setStatus(FocStatus.SUCCESS);

        ConsumerKafkaDto dto = new ConsumerKafkaDto()
                .setRequestId("req-extra")
                .setStatus("SUCCESS")
                .setEvents(List.of(event1, unknownEvent));

        GruValidationResult result = validateService.validate(dto, List.of(row1));

        assertEquals(1, result.rowsToUpdate().size());
    }
}

