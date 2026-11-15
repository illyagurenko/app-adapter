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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * сервис выборки большой пачки записей из бд, разбивки на подпачки по 100 штук
 * и перевода их в статус IN_PROCESS с фиксацией уникального requestId в SVFE_LOAD_ID.
 */
@Service
@RequiredArgsConstructor
public class GruBatchingService {

    private final GruVistaDao gruVistaDao;
    private final AdapterEntityFactory adapterEntityFactory;

    /**
     * изолированно блокирует большую порцию записей в бд (до fetchLimit),
     * нарезает их на подпачки по chunkSize (100 штук),
     * для каждой пачки генерирует свой уникальный requestId, проставляет его в SVFE_LOAD_ID
     * и переводит записи в статус IN_PROCESS.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ProducerKafkaDto> batchAndUpdate(int fetchLimit, int chunkSize) {
        List<GruVistaTab> allRows = gruVistaDao.fetchBatchForUpdate(fetchLimit);

        if (allRows == null || allRows.isEmpty()) {
            return Collections.emptyList();
        }

        List<ProducerKafkaDto> dtos = new ArrayList<>();

        for (int i = 0; i < allRows.size(); i += chunkSize) {
            List<GruVistaTab> chunk = allRows.subList(i, Math.min(i + chunkSize, allRows.size()));
            String requestId = UUID.randomUUID().toString();

            List<Long> chunkIds = chunk.stream()
                    .map(GruVistaTab::getId)
                    .toList();

            gruVistaDao.updateBatchStatusAndSvfeLoadId(chunkIds, FocStatus.IN_PROCESS, requestId);

            ProducerKafkaDto dto = adapterEntityFactory.createProducerKafkaDto(chunk, requestId);
            dtos.add(dto);
        }

        return dtos;
    }
}