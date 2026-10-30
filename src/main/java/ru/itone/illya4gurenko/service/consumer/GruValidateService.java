package ru.itone.illya4gurenko.service.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.GruRejectDao;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.*;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * сервис валидации ответа от kafka и применения изменений к таблицам GRU_VISTA_TAB и GRU_REJECT_TAB.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GruValidateService {

    private final GruVistaDao gruVistaDao;
    private final GruRejectDao gruRejectDao;
    private final AdapterEntityFactory adapterEntityFactory;

    /**
     * сопоставляет отправленные и полученные данные, обновляет балансы при успехе либо фиксирует брак при ошибках.
     */
    @Transactional
    public void processResult(ConsumerKafkaDto consumerDto, ProducerKafkaDto sentDto) {
        if ("ERROR".equalsIgnoreCase(consumerDto.getStatus()) || consumerDto.getEvents() == null) {
            log.warn("All batch with status ERROR. RequestId={}", consumerDto.getRequestId());
            String errorReason = consumerDto.getError() != null ? consumerDto.getError().getMessage() : "Общая ошибка пачки";
            rejectEntireBatch(sentDto, errorReason);
            return;
        }

        Set<Long> sentIds = sentDto.getEvents().stream().map(ProducerEventDto::getId).collect(Collectors.toSet());
        Set<Long> receivedIds = consumerDto.getEvents().stream().map(ConsumerEventDto::getEntityId).collect(Collectors.toSet());

        // проверка полноты пачки: если хотя бы один id потерян, вся пачка бракуется по ТЗ
        if (!receivedIds.containsAll(sentIds)) {
            log.error("Not all ENTITY_IDs received. Sent: {}, Got: {}", sentIds, receivedIds);
            rejectEntireBatch(sentDto, "Not all rows returned in response");
            return;
        }

        for (ConsumerEventDto event : consumerDto.getEvents()) {
            Long entityId = event.getEntityId();

            // ИСПРАВЛЕНО: безопасное сравнение вместо event.getStatus().name() во избежание NPE
            if (FocStatus.SUCCESS.equals(event.getStatus())) {
                EventDataDto data = event.getData();
                gruVistaDao.updateBalanceAndStatus(
                        entityId,
                        data != null ? data.getOldTbal() : null,
                        data != null ? data.getNewTbal() : null,
                        FocStatus.SUCCESS
                );
            } else {
                gruVistaDao.updateStatusByIds(List.of(entityId), FocStatus.ERROR);
                GruRejectTab reject = adapterEntityFactory.createRejectTab(event, "Unknown error");
                gruRejectDao.save(reject);
            }
        }
    }

    /**
     * бракует целиком всю пачку отправленных записей с указанием причины.
     */
    private void rejectEntireBatch(ProducerKafkaDto sentDto, String reason) {
        List<Long> sentIds = sentDto.getEvents().stream().map(ProducerEventDto::getId).toList();
        gruVistaDao.updateStatusByIds(sentIds, FocStatus.ERROR);

        for (ProducerEventDto event : sentDto.getEvents()) {
            GruRejectTab reject = adapterEntityFactory.createRejectTab(event, reason);
            gruRejectDao.save(reject);
        }
    }
}