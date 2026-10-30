package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.repository.GruRejectTabRepository;

/**
 * слой доступа к таблице забракованных записей (таблица GRU_REJECT_TAB).
 */
@Component
@RequiredArgsConstructor
public class GruRejectDao {
    private final GruRejectTabRepository repository;

    /**
     * сохраняет информацию об отклоненной или ошибочной записи.
     */
    public void save(GruRejectTab rejectTab) {
        repository.save(rejectTab);
    }
}