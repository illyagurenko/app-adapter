package ru.itone.illya4gurenko.service.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dto.ConsumerEventDto;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.EventDataDto;
import ru.itone.illya4gurenko.dto.GruValidationResult;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * сервис чистой бизнес-валидации ответа от kafka без прямого обращения к бд.
 * Формирует итоговые списки сущностей для пакетного обновления и отбраковки в GruConsumerService.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GruValidateService {

    private final AdapterEntityFactory adapterEntityFactory;

    /**
     * сопоставляет отправленные строки из бд с полученным ответом от kafka через удаление из мапы (map.remove).
     * Возвращает готовые списки записей для обновления GRU_VISTA_TAB и вставки в GRU_REJECT_TAB.
     */
    public GruValidationResult validate(ConsumerKafkaDto consumerDto, List<GruVistaTab> sentRows) {
        if (consumerDto == null || sentRows == null || sentRows.isEmpty()) {
            return GruValidationResult.empty();
        }

        String requestId = consumerDto.getRequestId();

        if ("ERROR".equalsIgnoreCase(consumerDto.getStatus()) || consumerDto.getEvents() == null) {
            log.warn("All batch with status ERROR. RequestId={}", requestId);
            String errorReason = consumerDto.getError() != null
                    ? consumerDto.getError().getMessage()
                    : "Общая ошибка пачки";
            return rejectEntireBatch(sentRows, requestId, errorReason);
        }

        Map<Long, GruVistaTab> remainingSentRows = new HashMap<>(sentRows.size() * 2);
        for (GruVistaTab row : sentRows) {
            remainingSentRows.put(row.getId(), row);
        }

        List<GruVistaTab> toUpdate = new ArrayList<>(sentRows.size());
        List<GruRejectTab> toReject = new ArrayList<>();

        for (ConsumerEventDto event : consumerDto.getEvents()) {
            Long entityId = event.getEntityId();
            GruVistaTab row = remainingSentRows.remove(entityId);

            if (row == null) {
                log.warn("Unexpected or duplicate ENTITY_ID={} in response for requestId={}", entityId, requestId);
                continue;
            }

            toUpdate.add(buildUpdatedRow(row, event));

            if (!FocStatus.SUCCESS.equals(event.getStatus())) {
                toReject.add(adapterEntityFactory.createRejectTab(event, row, requestId, "Unknown error"));
            }
        }

        // проверка полноты пачки: если в мапе остались записи, часть отправленных строк не вернулась -> брак всей пачки по ТЗ
        if (!remainingSentRows.isEmpty()) {
            log.error("Not all ENTITY_IDs received for requestId={}. Missing IDs: {}",
                    requestId, remainingSentRows.keySet());
            return rejectEntireBatch(sentRows, requestId, "Not all rows returned in response");
        }

        return new GruValidationResult(toUpdate, toReject, false, null);
    }

    /**
     * создает новый экземпляр GruVistaTab для обновления в бд, не мутируя исходную сущность sentRows
     * (чтобы сохранить исходные данные на случай отбраковки всей пачки в конце проверки).
     */
    private GruVistaTab buildUpdatedRow(GruVistaTab originalRow, ConsumerEventDto event) {
        if (FocStatus.SUCCESS.equals(event.getStatus())) {
            EventDataDto data = event.getData();
            return new GruVistaTab()
                    .setId(originalRow.getId())
                    .setOldTBal(data != null ? data.getOldTbal() : null)
                    .setNewTBal(data != null ? data.getNewTbal() : null)
                    .setFocStatus(FocStatus.SUCCESS);
        }

        return new GruVistaTab()
                .setId(originalRow.getId())
                .setOldTBal(originalRow.getOldTBal())
                .setNewTBal(originalRow.getNewTBal())
                .setFocStatus(FocStatus.ERROR);
    }

    /**
     * формирует списки отбраковки целиком для всей пачки отправленных записей с указанием причины.
     */
    private GruValidationResult rejectEntireBatch(List<GruVistaTab> sentRows, String requestId, String reason) {
        List<GruVistaTab> toUpdate = new ArrayList<>(sentRows.size());
        List<GruRejectTab> toReject = new ArrayList<>(sentRows.size());

        for (GruVistaTab row : sentRows) {
            toUpdate.add(new GruVistaTab()
                    .setId(row.getId())
                    .setOldTBal(row.getOldTBal())
                    .setNewTBal(row.getNewTBal())
                    .setFocStatus(FocStatus.ERROR));
            toReject.add(adapterEntityFactory.createRejectTab(row, requestId, reason));
        }

        return new GruValidationResult(toUpdate, toReject, true, reason);
    }
}
