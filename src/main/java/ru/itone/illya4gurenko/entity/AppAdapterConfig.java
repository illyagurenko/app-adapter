package ru.itone.illya4gurenko.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.EntityType;
import ru.itone.illya4gurenko.entity.enums.EventType;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import java.time.LocalDateTime;

/**
 * сущность таблицы конфигурации адаптера APP_ADAPTER_CONFIG.
 */
@Data
@Accessors(chain = true)
public class AppAdapterConfig {
    private Long id;

    private MsgType systemId;

    private EventType eventType;

    private EntityType entityType;

    private String topicIn;

    private Long statusIn;

    private String topicOut;

    private Long statusOut;

    private LocalDateTime insTs;

    private LocalDateTime updTs;

}