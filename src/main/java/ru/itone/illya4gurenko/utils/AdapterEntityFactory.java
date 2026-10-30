package ru.itone.illya4gurenko.utils;

import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.dto.ConsumerEventDto;
import ru.itone.illya4gurenko.dto.ProducerEventDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.EntityType;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * фабричный компонент для создания сущностей бд и dto объектов адаптера.
 */
@Component
public class AdapterEntityFactory {

    /**
     * создает новую запись транзакции при отправке батча сообщений.
     */
    public AppAdapterTrans createTrans(ProducerKafkaDto dto, String json) {
        return new AppAdapterTrans()
                .setSystemId(dto.getSystemId())
                .setRequestId(dto.getRequestId())
                .setEventType(dto.getEventType())
                .setData(json)
                .setStatus(FocStatus.IN_PROCESS)
                .setInsTs(LocalDateTime.now());
    }

    /**
     * создает запись журнала аудита ввода-вывода (IO_MSGS).
     */
    public AppAdapterIoMsgs createIoMsg(Long transId, Dir dir, String json, Long nodeId) {
        return new AppAdapterIoMsgs()
                .setTransId(transId)
                .setMsgType(MsgType.GRU)
                .setDir(dir)
                .setMsg(json)
                .setInsTs(LocalDateTime.now())
                .setNodeId(nodeId);
    }

    /**
     * создает запись в таблице брака на основе отправленного события (при ошибке всей пачки).
     */
    public GruRejectTab createRejectTab(ProducerEventDto eventDto, String errorMsg) {
        return new GruRejectTab()
                .setVistaTabId(eventDto.getId())
                .setSystemAccount(eventDto.getSystemAccount())
                .setRejectDesc(errorMsg)
                .setFrontStatus("ERR")
                .setFrontTimestamp(LocalDateTime.now());
    }

    /**
     * создает запись в таблице брака на основе полученного ответа из кафки (при ошибке конкретной строки).
     */
    public GruRejectTab createRejectTab(ConsumerEventDto eventDto, String defaultErrorMsg) {
        String errorMsg = eventDto.getError() != null
                ? (eventDto.getError().getCode() + ": " + eventDto.getError().getMessage())
                : defaultErrorMsg;

        return new GruRejectTab()
                .setVistaTabId(eventDto.getEntityId())
                .setSystemAccount(eventDto.getEntityValue())
                .setRejectDesc(errorMsg)
                .setFrontStatus("ERR")
                .setFrontTimestamp(LocalDateTime.now());
    }

    /**
     * создает исходящий корневой dto для отправки в kafka на основе списка строк из бд.
     */
    public ProducerKafkaDto createProducerKafkaDto(List<GruVistaTab> rows) {
        List<ProducerEventDto> events = rows.stream()
                .map(this::toEventDto)
                .toList();

        return new ProducerKafkaDto()
                .setActualTimestamp(System.currentTimeMillis())
                .setSystemId(MsgType.GRU)
                .setRequestId(UUID.randomUUID().toString())
                .setEventType(EventType.BALANCE)
                .setEntityType(EntityType.ACCOUNT)
                .setEvents(events);
    }

    /**
     * преобразует одну строку сущности GruVistaTab в событие ProducerEventDto.
     */
    private ProducerEventDto toEventDto(GruVistaTab row) {
        return new ProducerEventDto()
                .setId(row.getId())
                .setSystemAccount(row.getSystemAccount())
                .setCurrency(row.getCurrency())
                .setXalfa(row.getXalfa())
                .setOperation(row.getOperation());
    }
}