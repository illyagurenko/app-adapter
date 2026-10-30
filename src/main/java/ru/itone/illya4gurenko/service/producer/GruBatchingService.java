package ru.itone.illya4gurenko.service.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

import java.util.List;

/**
 * сервис выборки пачки записей из бд и перевода их в промежуточный статус IN_PROCESS.
 */
@Service
@RequiredArgsConstructor
public class GruBatchingService {

    private final GruVistaDao gruVistaDao;
    private final AdapterEntityFactory adapterEntityFactory;

    /**
     * изолированно блокирует порцию записей в бд, переводит их в статус IN_PROCESS и маппит в dto.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ProducerKafkaDto batchAndUpdate(int batchSize) {
        List<GruVistaTab> rows = gruVistaDao.fetchBatchForUpdate(batchSize);

        if (rows == null || rows.isEmpty()) {
            return null;
        }

        List<Long> ids = rows.stream()
                .map(GruVistaTab::getId)
                .toList();

        gruVistaDao.updateStatusByIds(ids, FocStatus.IN_PROCESS);

        return adapterEntityFactory.createProducerKafkaDto(rows);
    }
}