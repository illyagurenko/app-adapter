package ru.itone.illya4gurenko.dto;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.Type;

import java.math.BigDecimal;

/**
 * dto с финансовыми данными остатков до и после выполнения операции.
 */
@Data
@Accessors(chain = true)
public class EventDataDto {
    private BigDecimal oldTbal;
    private BigDecimal newTbal;
    private Type operation;
}