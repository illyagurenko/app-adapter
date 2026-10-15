package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.service.sender.KafkaProducerSender;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;
import ru.itone.illya4gurenko.utils.MapperUtils;

@Service
@RequiredArgsConstructor
@Slf4j
public class GruProducerService {

    @Value("${batch.size}")
    private int batchSize;

    @Value("${pc.number}")
    private Long nodeId;

    private final BatchingService batchingService;
    private final MapperUtils mapperUtils;
    private final AdapterEntityFactory adapterEntityFactory;
    private final KafkaProducerSender kafkaProducerSender;

    private final ProducerStatusService producerStatusService;

    public boolean produce(String topicOut) {

        ProducerKafkaDto curDto = batchingService.batchAndUpdate(batchSize);

        if (curDto == null || curDto.getEvents() == null || curDto.getEvents().isEmpty()) {
            return false;
        }

        log.info("found batch {} rows requestId={}", curDto.getEvents().size(), curDto.getRequestId());

        String json = mapperUtils.tryMappingToStr(curDto);

        if (json == null || json.isEmpty()) {
            log.error("failed to serialize producer message requestId={}", curDto.getRequestId());
            return false;
        }

        AppAdapterTrans trans = adapterEntityFactory.createTrans(curDto, json);

        producerStatusService.saveInitialTrans(trans);

        try {
            kafkaProducerSender.send(
                    topicOut,
                    curDto.getRequestId(),
                    json
            );

            producerStatusService.saveSuccessLog(trans, json, nodeId);

            log.info("request successfully sent to Kafka. requestId={}, topic={}, status={}",
                    curDto.getRequestId(), topicOut, trans.getStatus());

            return true;

        } catch (Exception e) {

            log.error("failed to send request to kafka. requestId={}", curDto.getRequestId(), e);

            String errorMsg = e.getMessage() != null ? e.getMessage() : "kafka producer error";
            producerStatusService.markAsError(trans, errorMsg);

            return true;
        }
    }
}