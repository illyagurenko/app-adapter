package ru.itone.illya4gurenko.controller.dto;

/**
 * dto ответа административного контроллера с информацией о состоянии сервиса.
 *
 * @param message информационное сообщение о результате операции
 * @param running флаг активности фоновых процессов (продюсеров/консьюмеров)
 * @param producers количество запущенных потоков продюсера
 * @param consumers количество запущенных потоков консьюмера
 */
public record AdminResponse(
        String message,
        boolean running,
        int producers,
        int consumers
) {
}