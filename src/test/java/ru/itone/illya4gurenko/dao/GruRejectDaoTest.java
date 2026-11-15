package ru.itone.illya4gurenko.dao;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.entity.GruRejectTab;
import ru.itone.illya4gurenko.repository.GruRejectTabRepository;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GruRejectDaoTest {

    private final GruRejectTabRepository repository = Mockito.mock(GruRejectTabRepository.class);
    private final SqlSessionFactory factory = Mockito.mock(SqlSessionFactory.class);
    private final GruRejectDao dao = new GruRejectDao(repository, factory);

    @Test
    @DisplayName("сохранение одной записи реджекта")
    void testSave() {
        GruRejectTab tab = new GruRejectTab();
        dao.save(tab);
        verify(repository).save(tab);
    }

    @Test
    @DisplayName("пропуск сохранения пустого батча реджектов")
    void testSaveBatchEmpty() {
        assertDoesNotThrow(() -> dao.saveBatch(Collections.emptyList()));
    }

    @Test
    @DisplayName("успешное батчевое сохранение реджектов")
    void testSaveBatchSuccess() {
        SqlSession session = Mockito.mock(SqlSession.class);
        GruRejectTabRepository mapper = Mockito.mock(GruRejectTabRepository.class);
        when(factory.openSession(ExecutorType.BATCH, false)).thenReturn(session);
        when(session.getMapper(GruRejectTabRepository.class)).thenReturn(mapper);

        GruRejectTab tab = new GruRejectTab().setVistaTabId(1L);
        dao.saveBatch(List.of(tab));

        verify(session).commit();
    }

    @Test
    @DisplayName("ошибка при батчевом сохранении реджектов")
    void testSaveBatchError() {
        SqlSession session = Mockito.mock(SqlSession.class);
        when(factory.openSession(ExecutorType.BATCH, false)).thenReturn(session);
        when(session.getMapper(GruRejectTabRepository.class)).thenThrow(new RuntimeException("DB error"));

        GruRejectTab tab = new GruRejectTab().setVistaTabId(1L);
        Assertions.assertThrows(IllegalStateException.class, () -> dao.saveBatch(List.of(tab)));
    }
}

