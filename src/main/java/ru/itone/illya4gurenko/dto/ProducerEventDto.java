package ru.itone.illya4gurenko.dto;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.Type;

import java.math.BigDecimal;

/**
 * dto отдельной финансовой операции для формирования исходящего пакета в кафку.
 */
@Data
@Accessors(chain = true)
public class ProducerEventDto {
    private Long id;
    private String systemAccount;
    private String currency;
    private BigDecimal xalfa;
    private Type operation;

}