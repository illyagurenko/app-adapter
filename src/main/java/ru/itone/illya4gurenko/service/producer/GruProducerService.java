package ru.itone.illya4gurenko.service.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ProducerEventDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;
import ru.itone.illya4gurenko.utils.MapperUtils;

import java.util.List;

/**
 * основной сервис координации процесса отправки: получение большой пачки, нарезка по 100 штук,
 * сериализация, учет транзакций и отправка каждого пакета в топик.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GruProducerService {

    @Value("${batch.fetch-size:10000}")
    private int fetchSize;

    @Value("${batch.size:100}")
    private int batchSize;

    @Value("${pc.number}")
    private Long nodeId;

    private final GruBatchingService batchingService;
    private final MapperUtils mapperUtils;
    private final AdapterEntityFactory adapterEntityFactory;
    private final KafkaProducerSender kafkaProducerSender;
    private final ProducerStatusService producerStatusService;
    private final GruVistaDao gruVistaDao;

    /**
     * выполняет один цикл вычитки большой пачки, нарезки по batchSize (100 штук),
     * создания транзакций и отправки сообщений в kafka.
     */
    public boolean produce(String topicOut) {
        long begin = System.currentTimeMillis();
        List<ProducerKafkaDto> dtos = batchingService.batchAndUpdate(fetchSize, batchSize);
        long endBatchAndUpdate = System.currentTimeMillis() - begin;

        if (dtos == null || dtos.isEmpty()) {
            return false;
        }

        int totalRows = dtos.stream().mapToInt(d -> d.getEvents() != null ? d.getEvents().size() : 0).sum();
        log.info("TIME found big batch {} rows, split into {} sub-batches time: {}",
                totalRows, dtos.size(), endBatchAndUpdate);

        boolean hasSuccess = false;

        for (ProducerKafkaDto curDto : dtos) {
            if (curDto == null || curDto.getEvents() == null || curDto.getEvents().isEmpty()) {
                continue;
            }

            String json = mapperUtils.tryMappingToStr(curDto);

            if (json == null || json.isEmpty()) {
                log.error("failed to serialize producer message requestId={}", curDto.getRequestId());
                continue;
            }

            AppAdapterTrans trans = adapterEntityFactory.createTrans(curDto);

            try {
                producerStatusService.saveInitialTrans(trans);

                kafkaProducerSender.send(
                        topicOut,
                        curDto.getRequestId(),
                        json
                );
                //сохр в io_msg
                producerStatusService.saveSuccessLog(trans, json, nodeId);

                log.info("request successfully sent to Kafka. requestId={}, topic={}, status={}",
                        curDto.getRequestId(), topicOut, trans.getStatus());

                hasSuccess = true;

            } catch (Exception e) {
                log.error("failed to send request to kafka. requestId={}", curDto.getRequestId(), e);

                String errorMsg = e.getMessage() != null ? e.getMessage() : "kafka producer error";
                try {
                    producerStatusService.markAsError(trans, errorMsg);
                } catch (Exception ex) {
                    log.error("failed to mark trans as error for requestId={}", curDto.getRequestId(), ex);
                }

                // Мгновенный сброс строк пачки в GRU_VISTA_TAB обратно в WAIT при сбое отправки
                try {
                    List<Long> failedIds = curDto.getEvents().stream()
                            .map(ProducerEventDto::getId)
                            .toList();
                    gruVistaDao.updateBatchStatusAndSvfeLoadId(failedIds, FocStatus.WAIT, "None");
                    log.warn("Reset {} rows back to WAIT for failed requestId={}", failedIds.size(), curDto.getRequestId());
                } catch (Exception ex) {
                    log.error("failed to reset rows to WAIT for requestId={}", curDto.getRequestId(), ex);
                }
            }
        }

        return hasSuccess;
    }
}