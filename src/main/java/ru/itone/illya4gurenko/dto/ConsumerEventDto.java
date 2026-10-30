package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.FocStatus;

/**
 * dto отдельного события в ответе от кафки с результатом обработки строки.
 */
@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerEventDto {
    private String entityValue;
    private Long entityId;
    private EventDataDto data;
    private FocStatus status;
    private ErrorDto error;
}