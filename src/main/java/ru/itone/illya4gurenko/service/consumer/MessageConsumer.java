package ru.itone.illya4gurenko.service.consumer;

/**
 * функциональный интерфейс для абстрагирования обработки входящих строковых сообщений из kafka.
 */
@FunctionalInterface
public interface MessageConsumer {

    /**
     * обрабатывает полученную строку сообщения из kafka.
     */
    void consume(String message);
}