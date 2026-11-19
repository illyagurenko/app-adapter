package ru.itone.illya4gurenko.dao;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AppAdapterIoMsgsDaoTest {

    private final AppAdapterIoMsgsRepository repository = Mockito.mock(AppAdapterIoMsgsRepository.class);
    private final AppAdapterIoMsgsDao dao = new AppAdapterIoMsgsDao(repository);

    @Test
    @DisplayName("сохранение входящего сообщения в dao")
    void testSave() {
        AppAdapterIoMsgs msg = new AppAdapterIoMsgs();

        dao.save(msg);

        verify(repository).save(msg);
    }

    @Test
    @DisplayName("поиск записи io_msgs по requestId и направлению")
    void testFindByRequestIdAndDir() {
        AppAdapterIoMsgs expected = new AppAdapterIoMsgs().setTransId(1L).setDir(Dir.IN);
        when(repository.findByRequestIdAndDir("req-1", Dir.IN)).thenReturn(expected);

        AppAdapterIoMsgs actual = dao.findByRequestIdAndDir("req-1", Dir.IN);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("возврат null при пустых параметрах поиска io_msgs")
    void testFindByRequestIdAndDirWhenBlank() {
        assertNull(dao.findByRequestIdAndDir(" ", Dir.IN));
    }
}
