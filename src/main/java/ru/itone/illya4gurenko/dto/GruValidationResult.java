package ru.itone.illya4gurenko.dto;

import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.entity.GruVistaTab;

import java.util.Collections;
import java.util.List;

/**
 * результат валидации ответа kafka: списки записей для пакетного обновления и отбраковки в бд.
 */
public record GruValidationResult(
        List<GruVistaTab> rowsToUpdate,
        List<GruRejectTab> rowsToReject,
        boolean batchFailed,
        String errorReason
) {

    /**
     * возвращает пустой результат валидации без изменений в бд.
     */
    public static GruValidationResult empty() {
        return new GruValidationResult(Collections.emptyList(), Collections.emptyList(), false, null);
    }
}

