package ru.itone.illya4gurenko.dao;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.repository.GruVistaTabRepository;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GruVistaDaoTest {

    private final GruVistaTabRepository repository = Mockito.mock(GruVistaTabRepository.class);
    private final SqlSessionFactory factory = Mockito.mock(SqlSessionFactory.class);
    private final GruVistaDao dao = new GruVistaDao(repository, factory);

    @Test
    @DisplayName("выборка пакета записей для отправки")
    void testFetchBatchForUpdate() {
        when(repository.fetchBatchForUpdate(10)).thenReturn(Collections.emptyList());
        List<GruVistaTab> result = dao.fetchBatchForUpdate(10);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("поиск записей при пустом svfeLoadId")
    void testFindBySvfeLoadIdBlank() {
        List<GruVistaTab> result = dao.findBySvfeLoadId(" ");
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("поиск записей по заполненному svfeLoadId")
    void testFindBySvfeLoadIdPresent() {
        GruVistaTab tab = new GruVistaTab().setId(1L);
        when(repository.findBySvfeLoadId("req1")).thenReturn(List.of(tab));

        List<GruVistaTab> result = dao.findBySvfeLoadId("req1");

        assertEquals(1, result.size());
    }

    @Test
    @DisplayName("пакетное обновление статуса и svfeLoadId для пустого списка")
    void testUpdateBatchStatusAndSvfeLoadId() {
        dao.updateBatchStatusAndSvfeLoadId(Collections.emptyList(), FocStatus.WAIT, "none");
        verify(repository, Mockito.never()).updateBatchStatusAndSvfeLoadId(any(), any(), any());
    }

    @Test
    @DisplayName("обновление статуса по списку идентификаторов для пустого списка")
    void testUpdateStatusByIds() {
        dao.updateStatusByIds(Collections.emptyList(), FocStatus.WAIT);
        verify(repository, Mockito.never()).updateStatusByIds(any(), any());
    }

    @Test
    @DisplayName("обновление баланса и статуса одной записи")
    void testUpdateBalanceAndStatus() {
        dao.updateBalanceAndStatus(1L, BigDecimal.ZERO, BigDecimal.ONE, FocStatus.SUCCESS);
        verify(repository).updateBalanceAndStatus(1L, BigDecimal.ZERO, BigDecimal.ONE, FocStatus.SUCCESS);
    }

    @Test
    @DisplayName("сброс зависших записей")
    void testResetOrphanedStuckRecords() {
        when(repository.resetOrphanedStuckRecords()).thenReturn(3);
        int result = dao.resetOrphanedStuckRecords();
        assertEquals(3, result);
    }

    @Test
    @DisplayName("финальное обновление gru для пустого списка")
    void testFinalUpdateGruEmpty() {
        assertDoesNotThrow(() -> dao.finalUpdateGru(Collections.emptyList()));
    }

    @Test
    @DisplayName("успешное финальное обновление gru")
    void testFinalUpdateGruSuccess() {
        SqlSession session = Mockito.mock(SqlSession.class);
        GruVistaTabRepository mapper = Mockito.mock(GruVistaTabRepository.class);
        when(factory.openSession(ExecutorType.BATCH, false)).thenReturn(session);
        when(session.getMapper(GruVistaTabRepository.class)).thenReturn(mapper);

        GruVistaTab tab = new GruVistaTab().setId(1L).setOldTBal(BigDecimal.ONE).setNewTBal(BigDecimal.TEN).setFocStatus(FocStatus.SUCCESS);
        dao.finalUpdateGru(List.of(tab));

        verify(session).commit();
    }

    @Test
    @DisplayName("ошибка при финальном обновлении gru")
    void testFinalUpdateGruError() {
        SqlSession session = Mockito.mock(SqlSession.class);
        when(factory.openSession(ExecutorType.BATCH, false)).thenReturn(session);
        when(session.getMapper(GruVistaTabRepository.class)).thenThrow(new RuntimeException("DB error"));

        GruVistaTab tab = new GruVistaTab().setId(1L);
        Assertions.assertThrows(IllegalStateException.class, () -> dao.finalUpdateGru(List.of(tab)));
    }
}

