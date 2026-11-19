package ru.itone.illya4gurenko.controller.dto;

import java.time.ZonedDateTime;

/**
 * dto записи аудита из сервиса app-changer (таблица APP_CHANGER_IO_MSGS).
 */
public record ChangerIoMsgDto(
        Long id,
        Long transId,
        String requestId,
        String protocolType,
        String msgType,
        String dir,
        String nodeId,
        ZonedDateTime insTs
) {}
