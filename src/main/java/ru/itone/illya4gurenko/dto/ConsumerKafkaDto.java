package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.EntityType;
import ru.itone.illya4gurenko.entity.enums.EventType;

import java.util.List;

/**
 * корневой dto входящего сообщения из кафки от внешней системы (например, SMART).
 */
@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerKafkaDto {
    private Long actualTimestamp;
    private String systemId;
    private String requestId;
    private EventType eventType;
    private EntityType entityType;
    private String status;
    private ErrorDto error;
    private List<ConsumerEventDto> events;
}