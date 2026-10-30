package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;

/**
 * слой доступа к аудиту входящих и исходящих сырых сообщений (таблица APP_ADAPTER_IO_MSGS).
 */
@Component
@RequiredArgsConstructor
public class AppAdapterIoMsgsDao {

    private final AppAdapterIoMsgsRepository repository;

    /**
     * сохраняет запись входящего или исходящего json-сообщения в аудит-таблицу.
     */
    public void save(AppAdapterIoMsgs ioMsgs) {
        repository.save(ioMsgs);
    }
}