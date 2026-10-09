package ru.itone.illya4gurenko.controller.dto;

public record AdminResponse(
        String message,
        boolean running,
        int producers,
        int consumers
) {
}