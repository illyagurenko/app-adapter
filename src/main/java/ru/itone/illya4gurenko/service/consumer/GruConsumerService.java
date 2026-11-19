package ru.itone.illya4gurenko.service.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dao.GruRejectDao;
import ru.itone.illya4gurenko.dao.GruVistaDao;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.GruValidationResult;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.GruVistaTab;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.MapperUtils;

import java.util.List;

/**
 * основной сервис обработки входящих сообщений из kafka: десериализация, проверка идемпотентности,
 * вызов валидации и пакетное сохранение результатов в бд.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GruConsumerService implements MessageConsumer {

    @Value("${pc.number}")
    private Long numberPC;

    private final MapperUtils mapperUtils;
    private final AppAdapterTransDao transDao;
    private final GruVistaDao gruVistaDao;
    private final GruRejectDao gruRejectDao;
    private final TransactionTemplate transactionTemplate;
    private final GruValidateService gruValidateService;
    private final ConsumerStatusService consumerStatusService;

    /**
     * выполняет бизнес-обработку одного полученного json-ответа от kafka.
     */
    @Override
    public void consume(String json) {

        log.debug("consume json:\n{}", json);

        ConsumerKafkaDto consumerDto = mapperUtils.tryMappingToConsumerKafkaDto(json);

        if (consumerDto == null) {
            log.error("failed to parse ConsumerKafkaDto from Kafka message");
            return;
        }

        String requestId = consumerDto.getRequestId();

        if (requestId == null || requestId.isBlank()) {
            log.error("kafka response does not contain requestId");
            return;
        }

        AppAdapterTrans trans = transDao.findByRequestId(requestId);

        if (trans == null) {
            log.warn("trans with requestId='{}' not found in APP_ADAPTER_TRANS. Validating GRU_VISTA_TAB directly...", requestId);
            try {
                validateAndPersist(consumerDto);
                log.info("consumer successfully processed orphaned response for requestId={}", requestId);
            } catch (Exception e) {
                log.error("failed to process orphaned response for requestId={}", requestId, e);
            }
            return;
        }

        // проверка идемпотентности во избежание повторной обработки дубликатов из kafka
        if (trans.getStatus() == FocStatus.SUCCESS || trans.getStatus() == FocStatus.ERROR) {
            log.warn("message with requestId='{}' already processed with status={}. Skipping duplicate.", requestId, trans.getStatus());
            return;
        }

        log.info("consumer starts processing requestId={}, previousStatus={}", requestId, trans.getStatus());

        consumerStatusService.startProcessing(trans, json, numberPC);

        try {
            long begin = System.currentTimeMillis();
            GruValidationResult result = validateAndPersist(consumerDto);
            long endValidateIDs = System.currentTimeMillis() - begin;
            log.info("Validate data in consumer: {}", endValidateIDs);

            if ("ERROR".equalsIgnoreCase(consumerDto.getStatus()) || result.batchFailed()) {
                String errorDesc = result.errorReason() != null ? result.errorReason() : "batch error processed";
                consumerStatusService.updateFinalStatus(trans, FocStatus.ERROR, null, errorDesc);
            } else {
                long begin1 = System.currentTimeMillis();
                consumerStatusService.updateFinalStatus(trans, FocStatus.SUCCESS, "0", "SUCCESS");
                long endUpdateStatusSuccess = System.currentTimeMillis() - begin1;
                log.info("final operation success: {}", endUpdateStatusSuccess);
            }

            log.info("consumer finished requestId={}, finalStatus={} with time: {}", requestId, trans.getStatus(), endValidateIDs);

        } catch (Exception e) {
            log.error("technical error processing requestId={}", requestId, e);

            String errorMsg = e.getMessage() != null ? e.getMessage() : "technical error";
            consumerStatusService.updateFinalStatus(trans, FocStatus.ERROR, "500", "consumer failed: " + errorMsg);

            throw new IllegalStateException("consumer processing failed for requestId=" + requestId, e);
        }
    }

    /**
     * вычитывает отправленные строки из бд, запускает чистую валидацию
     * и атомарно сохраняет все изменения в GRU_VISTA_TAB и GRU_REJECT_TAB пакетным JDBC-запросом
     */
    private GruValidationResult validateAndPersist(ConsumerKafkaDto consumerDto) {
        String requestId = consumerDto.getRequestId();

        long t0 = System.currentTimeMillis();
        List<GruVistaTab> sentRows = gruVistaDao.findBySvfeLoadId(requestId);
        long tFind = System.currentTimeMillis() - t0;

        if (sentRows == null || sentRows.isEmpty()) {
            log.error("No records found in GRU_VISTA_TAB for SVFE_LOAD_ID={}", requestId);
            return GruValidationResult.empty();
        }

        GruValidationResult result = gruValidateService.validate(consumerDto, sentRows);

        long t1 = System.currentTimeMillis();
        if (!result.rowsToUpdate().isEmpty() || !result.rowsToReject().isEmpty()) {
            //обновление батчем
            transactionTemplate.executeWithoutResult(status -> {
                gruVistaDao.finalUpdateGru(result.rowsToUpdate());
                if (!result.rowsToReject().isEmpty()) {
                    gruRejectDao.saveBatch(result.rowsToReject());
                }
            });
        }
        long tUpdates = System.currentTimeMillis() - t1;

        log.info("TIMING requestId={}: findBySvfeLoadId={} ms, batchPersist={} ms", requestId, tFind, tUpdates);
        return result;
    }
}
