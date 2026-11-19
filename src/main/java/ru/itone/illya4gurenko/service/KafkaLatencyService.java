package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.itone.illya4gurenko.controller.dto.ChangerIoMsgDto;
import ru.itone.illya4gurenko.controller.dto.KafkaLatencyResponse;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.enums.Dir;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * сервис расчета времени доставки сообщений через kafka между app-adapter и app-changer.
 */
@Service
@RequiredArgsConstructor
public class KafkaLatencyService {

    private static final String DIR_CHANGER_TO_ADAPTER = "CHANGER_TO_ADAPTER";
    private static final String DIR_PRODUCER_TO_CHANGER = "PRODUCER_TO_CHANGER";
    private static final ZoneId ADAPTER_ZONE = ZoneId.of("Europe/Moscow");

    private final ChangerRestClient changerRestClient;
    private final AppAdapterIoMsgsDao ioMsgsDao;

    /**
     * высчитывает время получения адаптером ответа из kafka:
     * сравнивает время отправки последнего OUT-сообщения из app-changer и время получения IN-сообщения в app-adapter.
     */
    public KafkaLatencyResponse calculateReceiveFromKafkaLatency() {
        ChangerIoMsgDto changerOut = changerRestClient.getLatestIoMsg("OUT");
        if (changerOut == null || changerOut.requestId() == null || changerOut.insTs() == null) {
            return new KafkaLatencyResponse(DIR_CHANGER_TO_ADAPTER, "NO_CHANGER_DATA", null, null, null, null);
        }

        AppAdapterIoMsgs adapterIn = ioMsgsDao.findByRequestIdAndDir(changerOut.requestId(), Dir.IN);
        if (adapterIn == null || adapterIn.getInsTs() == null) {
            return new KafkaLatencyResponse(
                    DIR_CHANGER_TO_ADAPTER,
                    "NOT_RECEIVED_BY_ADAPTER_YET",
                    changerOut.requestId(),
                    changerOut.insTs(),
                    null,
                    null
            );
        }

        ZonedDateTime sentAt = changerOut.insTs();
        ZonedDateTime receivedAt = adapterIn.getInsTs().atZone(ADAPTER_ZONE).withZoneSameInstant(sentAt.getZone());
        long latencyMs = Math.abs(Duration.between(sentAt, receivedAt).toMillis());

        return new KafkaLatencyResponse(
                DIR_CHANGER_TO_ADAPTER,
                "OK",
                changerOut.requestId(),
                sentAt,
                receivedAt,
                latencyMs
        );
    }

    /**
     * высчитывает время получения записи от продюсера адаптера:
     * сравнивает время отправки OUT-сообщения продюсером app-adapter и время получения последнего IN-сообщения в app-changer.
     */
    public KafkaLatencyResponse calculateReceiveFromProducerLatency() {
        ChangerIoMsgDto changerIn = changerRestClient.getLatestIoMsg("IN");
        if (changerIn == null || changerIn.requestId() == null || changerIn.insTs() == null) {
            return new KafkaLatencyResponse(DIR_PRODUCER_TO_CHANGER, "NO_CHANGER_DATA", null, null, null, null);
        }

        AppAdapterIoMsgs adapterOut = ioMsgsDao.findByRequestIdAndDir(changerIn.requestId(), Dir.OUT);
        if (adapterOut == null || adapterOut.getInsTs() == null) {
            return new KafkaLatencyResponse(
                    DIR_PRODUCER_TO_CHANGER,
                    "NO_ADAPTER_DATA",
                    changerIn.requestId(),
                    null,
                    changerIn.insTs(),
                    null
            );
        }

        ZonedDateTime sentAt = adapterOut.getInsTs().atZone(ADAPTER_ZONE).withZoneSameInstant(changerIn.insTs().getZone());
        ZonedDateTime receivedAt = changerIn.insTs();
        long latencyMs = Math.abs(Duration.between(sentAt, receivedAt).toMillis());

        return new KafkaLatencyResponse(
                DIR_PRODUCER_TO_CHANGER,
                "OK",
                changerIn.requestId(),
                sentAt,
                receivedAt,
                latencyMs
        );
    }
}
