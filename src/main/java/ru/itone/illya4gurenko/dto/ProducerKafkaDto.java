package ru.itone.illya4gurenko.dto;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.EntityType;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.util.List;

/**
 * корневой dto исходящего сообщения с пачкой событий, отправляемого в кафку.
 */
@Data
@Accessors(chain = true)
public class ProducerKafkaDto {
    private Long actualTimestamp;
    private MsgType systemId;
    private String requestId;
    private EventType eventType;
    private EntityType entityType;
    private List<ProducerEventDto> events;

}