package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;
import ru.itone.illya4gurenko.utils.MapperUtils;

@Service
@RequiredArgsConstructor
@Slf4j
public class GruConsumerService {

    @Value("${pc.number}")
    private Long numberPC;

    private final MapperUtils mapperUtils;
    private final AppAdapterTransDao transDao;
    private final AppAdapterIoMsgsDao ioMsgsDao;
    private final AdapterEntityFactory adapterEntityFactory;
    private final GruValidateService gruValidateService;
    private final ConsumerStatusService consumerStatusService;

    @Transactional
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

        if (trans.getStatus() == FocStatus.SUCCESS || trans.getStatus() == FocStatus.ERROR) {
            log.warn("message with requestId='{}' already processed " + "with status={}. Skipping duplicate.", requestId, trans.getStatus());
            return;
        }

        log.info("consumer starts processing requestId={}, previousStatus={}", requestId, trans.getStatus());

        consumerStatusService.markInProcess(trans);
        AppAdapterIoMsgs ioMsg = adapterEntityFactory.createIoMsg(trans.getId(), Dir.IN, json, numberPC);
        ioMsgsDao.save(ioMsg);

        ProducerKafkaDto sentDto = mapperUtils.tryMappingToProducerKafkaDto(trans.getData());

        if (sentDto == null) {
            log.error("failed to parse sent data for requestId={}", requestId);

            trans.setStatus(FocStatus.ERROR)
                    .setRespDesc("failed to parse sent data");
            transDao.updateStatus(trans);

            return;
        }

        try {
            gruValidateService.processResult(consumerDto, sentDto);

            if ("ERROR".equalsIgnoreCase(consumerDto.getStatus())) {
                trans.setStatus(FocStatus.ERROR)
                        .setRespDesc("batch error processed");

            } else {

                trans.setStatus(FocStatus.SUCCESS)
                        .setRespCode("0")
                        .setRespDesc("SUCCESS");
            }

            transDao.updateStatus(trans);

            log.info("consumer finished requestId={}, finalStatus={}", requestId, trans.getStatus());

        } catch (Exception e) {
            log.error("technical error processing requestId={}", requestId, e);
            throw new IllegalStateException("consumer processing failed for requestId=" + requestId, e);
        }
    }
}