package ru.itone.illya4gurenko.dao;

import lombok.RequiredArgsConstructor;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.repository.GruRejectTabRepository;

import java.util.List;

/**
 * слой доступа к таблице забракованных записей (таблица GRU_REJECT_TAB).
 */
@Component
@RequiredArgsConstructor
public class GruRejectDao {
    private final GruRejectTabRepository repository;
    private final SqlSessionFactory factory;

    /**
     * сохраняет информацию об отклоненной или ошибочной записи.
     */
    public void save(GruRejectTab rejectTab) {
        repository.save(rejectTab);
    }

    /**
     * пакетно сохраняет список отклоненных или ошибочных записей в GRU_REJECT_TAB за один JDBC-батч.
     */
    @Transactional
    public void saveBatch(List<GruRejectTab> rejectTabs) {
        if (rejectTabs == null || rejectTabs.isEmpty()) {
            return;
        }
        try (SqlSession session = factory.openSession(ExecutorType.BATCH, false)) {
            try {
                GruRejectTabRepository mapper = session.getMapper(GruRejectTabRepository.class);
                for (GruRejectTab rejectTab : rejectTabs) {
                    mapper.saveBatchItem(rejectTab);
                }
                session.flushStatements();
                session.commit();
            } catch (Exception e) {
                try {
                    session.rollback();
                } catch (Exception rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw new IllegalStateException("Failed to batch insert into GRU_REJECT_TAB", e);
            }
        }
    }
}
