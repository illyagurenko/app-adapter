package ru.itone.illya4gurenko.service.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.MapperUtils;

/**
 * основной сервис обработки входящих сообщений из kafka: десериализация, проверка идемпотентности и обновление бизнес-данных.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GruConsumerService implements MessageConsumer {

    @Value("${pc.number}")
    private Long numberPC;

    private final MapperUtils mapperUtils;
    private final AppAdapterTransDao transDao;
    private final GruValidateService gruValidateService;
    private final ConsumerStatusService consumerStatusService;

    /**
     * выполняет бизнес-обработку одного полученного json-ответа от kafka.
     */
    @Override
    public void consume(String json) {

        log.info("consume json:\n{}", json);

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
            log.error("trans with requestId='{}' not found", requestId);
            return;
        }

        // проверка идемпотентности во избежание повторной обработки дубликатов из kafka
        if (trans.getStatus() == FocStatus.SUCCESS || trans.getStatus() == FocStatus.ERROR) {
            log.warn("message with requestId='{}' already processed with status={}. Skipping duplicate.", requestId, trans.getStatus());
            return;
        }

        log.info("consumer starts processing requestId={}, previousStatus={}", requestId, trans.getStatus());

        consumerStatusService.startProcessing(trans, json, numberPC);

        ProducerKafkaDto sentDto = mapperUtils.tryMappingToProducerKafkaDto(trans.getData());

        if (sentDto == null) {
            log.error("failed to parse sent data for requestId={}", requestId);
            consumerStatusService.updateFinalStatus(trans, FocStatus.ERROR, null, "failed to parse sent data");
            return;
        }

        try {
            gruValidateService.processResult(consumerDto, sentDto);

            if ("ERROR".equalsIgnoreCase(consumerDto.getStatus())) {
                consumerStatusService.updateFinalStatus(trans, FocStatus.ERROR, null, "batch error processed");
            } else {
                consumerStatusService.updateFinalStatus(trans, FocStatus.SUCCESS, "0", "SUCCESS");
            }

            log.info("consumer finished requestId={}, finalStatus={}", requestId, trans.getStatus());

        } catch (Exception e) {
            log.error("technical error processing requestId={}", requestId, e);

            String errorMsg = e.getMessage() != null ? e.getMessage() : "technical error";
            consumerStatusService.updateFinalStatus(trans, FocStatus.ERROR, "500", "consumer failed: " + errorMsg);

            throw new IllegalStateException("consumer processing failed for requestId=" + requestId, e);
        }
    }
}