package ru.itone.illya4gurenko.controller.dto;

import java.time.ZonedDateTime;

/**
 * dto результата расчета времени доставки сообщения через kafka между app-adapter и app-changer.
 */
public record KafkaLatencyResponse(
        String direction,
        String status,
        String requestId,
        ZonedDateTime sentAt,
        ZonedDateTime receivedAt,
        Long latencyMs
) {}
