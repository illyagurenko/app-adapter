package ru.itone.illya4gurenko.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.dto.ConsumerEventDto;
import ru.itone.illya4gurenko.dto.ErrorDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AdapterEntityFactoryTest {

    private final AdapterEntityFactory factory = new AdapterEntityFactory();

    @Test
    @DisplayName("создание сущности транзакции адаптера из kafka dto")
    void testCreateTrans() {
        ProducerKafkaDto dto = new ProducerKafkaDto()
                .setSystemId(MsgType.GRU)
                .setRequestId("req-1")
                .setEventType(EventType.BALANCE);

        AppAdapterTrans trans = factory.createTrans(dto);

        assertAll(
                () -> assertEquals("req-1", trans.getRequestId()),
                () -> assertEquals(MsgType.GRU, trans.getSystemId()),
                () -> assertEquals(EventType.BALANCE, trans.getEventType()),
                () -> assertEquals(FocStatus.IN_PROCESS, trans.getStatus())
        );
    }

    @Test
    @DisplayName("создание записи io_msgs")
    void testCreateIoMsg() {
        AppAdapterIoMsgs msg = factory.createIoMsg(100L, Dir.IN, "{}", 161L);

        assertAll(
                () -> assertEquals(100L, msg.getTransId()),
                () -> assertEquals(Dir.IN, msg.getDir()),
                () -> assertEquals("{}", msg.getMsg()),
                () -> assertEquals(161L, msg.getNodeId())
        );
    }

    @Test
    @DisplayName("создание записи reject_tab из строки vista")
    void testCreateRejectTabFromRow() {
        GruVistaTab row = new GruVistaTab()
                .setId(10L)
                .setSystemAccount("ACC_1")
                .setUterrario(100L)
                .setOldTBal(BigDecimal.ONE)
                .setNewTBal(BigDecimal.TEN)
                .setSvfeLoadId("svfe-1");

        GruRejectTab reject = factory.createRejectTab(row, "req-1", "Error reason");

        assertAll(
                () -> assertEquals(10L, reject.getVistaTabId()),
                () -> assertEquals("ACC_1", reject.getSystemAccount()),
                () -> assertEquals("req-1", reject.getSvfeLoadId()),
                () -> assertEquals("Error reason", reject.getRejectDesc())
        );
    }

    @Test
    @DisplayName("создание записи reject_tab из события с errorDto")
    void testCreateRejectTabFromEventWithErrorDto() {
        ConsumerEventDto event = new ConsumerEventDto()
                .setEntityId(15L)
                .setEntityValue("ACC_EVENT")
                .setError(new ErrorDto().setCode("400").setMessage("Bad account"));

        GruVistaTab row = new GruVistaTab()
                .setId(15L)
                .setSystemAccount("ACC_EVENT")
                .setOldTBal(BigDecimal.ZERO)
                .setNewTBal(BigDecimal.ONE);

        GruRejectTab reject = factory.createRejectTab(event, row, "req-event", "default err");

        assertAll(
                () -> assertEquals(15L, reject.getVistaTabId()),
                () -> assertEquals("ACC_EVENT", reject.getSystemAccount()),
                () -> assertEquals("400: Bad account", reject.getRejectDesc())
        );
    }

    @Test
    @DisplayName("создание ProducerKafkaDto с заданным requestId")
    void testCreateProducerKafkaDtoWithRequestId() {
        GruVistaTab row = new GruVistaTab()
                .setId(1L)
                .setSystemAccount("ACC1")
                .setCurrency("RUB")
                .setXalfa(BigDecimal.ONE)
                .setOperation(ru.itone.illya4gurenko.entity.enums.Type.DR);

        ProducerKafkaDto dto = factory.createProducerKafkaDto(List.of(row), "fixed-req");

        assertAll(
                () -> assertEquals("fixed-req", dto.getRequestId()),
                () -> assertEquals(1, dto.getEvents().size()),
                () -> assertEquals("ACC1", dto.getEvents().get(0).getSystemAccount())
        );
    }

    @Test
    @DisplayName("создание ProducerKafkaDto со случайным requestId")
    void testCreateProducerKafkaDtoRandomRequestId() {
        GruVistaTab row = new GruVistaTab().setId(2L).setSystemAccount("ACC2");
        ProducerKafkaDto dto = factory.createProducerKafkaDto(List.of(row));

        assertNotNull(dto.getRequestId());
    }
}

