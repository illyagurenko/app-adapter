package ru.itone.illya4gurenko.dto;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * dto ошибки с кодом и текстовым описанием проблемы.
 */
@Data
@Accessors(chain = true)
public class ErrorDto {
    private String code;
    private String message;
}