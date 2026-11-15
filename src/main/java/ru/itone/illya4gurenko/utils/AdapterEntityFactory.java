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
    public AppAdapterTrans createTrans(ProducerKafkaDto dto) {
        return new AppAdapterTrans()
                .setSystemId(dto.getSystemId())
                .setRequestId(dto.getRequestId())
                .setEventType(dto.getEventType())
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
    public GruRejectTab createRejectTab(GruVistaTab row, String requestId, String errorMsg) {
        return new GruRejectTab()
                .setVistaTabId(row.getId())
                .setSystemAccount(row.getSystemAccount())
                .setUterrario(row.getUterrario() != null ? row.getUterrario() : null)
                .setOldTBal(row.getOldTBal())
                .setNewTBal(row.getNewTBal())
                .setSvfeLoadId(requestId != null ? requestId : row.getSvfeLoadId())
                .setRejectDesc(errorMsg)
                .setFrontStatus("ERR")
                .setFrontTimestamp(LocalDateTime.now());
    }

    /**
     * создает запись в таблице брака на основе полученного ответа из кафки (при ошибке конкретной строки).
     */
    public GruRejectTab createRejectTab(ConsumerEventDto eventDto, GruVistaTab row, String requestId, String defaultErrorMsg) {
        String errorMsg = eventDto.getError() != null
                ? (eventDto.getError().getCode() + ": " + eventDto.getError().getMessage())
                : defaultErrorMsg;

        GruRejectTab reject = new GruRejectTab()
                .setVistaTabId(eventDto.getEntityId())
                .setSystemAccount(eventDto.getEntityValue())
                .setSvfeLoadId(requestId)
                .setRejectDesc(errorMsg)
                .setFrontStatus("ERR")
                .setFrontTimestamp(LocalDateTime.now());

        if (row != null) {
            reject.setUterrario(row.getUterrario() != null ? row.getUterrario() : null)
                    .setOldTBal(row.getOldTBal())
                    .setNewTBal(row.getNewTBal());
            if (reject.getSystemAccount() == null) {
                reject.setSystemAccount(row.getSystemAccount());
            }
        }

        return reject;
    }

    /**
     * создает исходящий корневой dto для отправки в kafka с заданным requestId на основе списка строк из бд.
     */
    public ProducerKafkaDto createProducerKafkaDto(List<GruVistaTab> rows, String requestId) {
        List<ProducerEventDto> events = rows.stream()
                .map(this::toEventDto)
                .toList();

        return new ProducerKafkaDto()
                .setActualTimestamp(System.currentTimeMillis())
                .setSystemId(MsgType.GRU)
                .setRequestId(requestId)
                .setEventType(EventType.BALANCE)
                .setEntityType(EntityType.ACCOUNT)
                .setEvents(events);
    }

    /**
     * создает исходящий корневой dto для отправки в kafka на основе списка строк из бд со случайным requestId.
     */
    public ProducerKafkaDto createProducerKafkaDto(List<GruVistaTab> rows) {
        return createProducerKafkaDto(rows, UUID.randomUUID().toString());
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