package ru.itone.illya4gurenko.dao;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.repository.AppAdapterIoMsgsRepository;

import static org.mockito.Mockito.verify;

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
}

