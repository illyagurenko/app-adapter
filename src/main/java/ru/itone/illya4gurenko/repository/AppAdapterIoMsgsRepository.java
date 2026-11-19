package ru.itone.illya4gurenko.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.enums.Dir;

import java.util.List;

/**
 * mybatis-репозиторий для аудита сообщений ввода-вывода (APP_ADAPTER_IO_MSGS).
 */
@Mapper
public interface AppAdapterIoMsgsRepository {

    /**
     * сохраняет запись входящего или исходящего json-сообщения.
     */
    void save(AppAdapterIoMsgs ioMsgs);

    /**
     * возвращает полный список записей аудита сообщений.
     */
    List<AppAdapterIoMsgs> findAll();

    /**
     * ищет запись аудита по requestId транзакции и направлению сообщения (IN / OUT).
     */
    AppAdapterIoMsgs findByRequestIdAndDir(@Param("requestId") String requestId,
                                           @Param("dir") Dir dir);
}
