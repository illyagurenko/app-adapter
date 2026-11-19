package ru.itone.illya4gurenko.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.controller.dto.ChangerIoMsgDto;
import ru.itone.illya4gurenko.controller.dto.KafkaLatencyResponse;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.enums.Dir;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class KafkaLatencyServiceTest {

    private final ChangerRestClient changerRestClient = Mockito.mock(ChangerRestClient.class);
    private final AppAdapterIoMsgsDao ioMsgsDao = Mockito.mock(AppAdapterIoMsgsDao.class);
    private final KafkaLatencyService service = new KafkaLatencyService(changerRestClient, ioMsgsDao);

    @Test
    @DisplayName("успешный расчет времени получения ответа адаптером из kafka")
    void testCalculateReceiveFromKafkaLatencyOk() {
        ZoneId zone = ZoneId.of("Europe/Moscow");
        ZonedDateTime sentAt = ZonedDateTime.of(2026, 11, 18, 12, 0, 0, 0, zone);
        ChangerIoMsgDto changerOut = new ChangerIoMsgDto(1L, 10L, "req-1", "KAFKA", "GRU", "OUT", "161", sentAt);
        AppAdapterIoMsgs adapterIn = new AppAdapterIoMsgs().setInsTs(sentAt.plusNanos(50_000_000).toLocalDateTime());

        when(changerRestClient.getLatestIoMsg("OUT")).thenReturn(changerOut);
        when(ioMsgsDao.findByRequestIdAndDir("req-1", Dir.IN)).thenReturn(adapterIn);

        KafkaLatencyResponse response = service.calculateReceiveFromKafkaLatency();

        assertAll(
                () -> assertEquals("OK", response.status()),
                () -> assertEquals("req-1", response.requestId()),
                () -> assertEquals(50L, response.latencyMs())
        );
    }

    @Test
    @DisplayName("расчет времени получения из kafka при отсутствии данных ченджера")
    void testCalculateReceiveFromKafkaLatencyNoChangerData() {
        when(changerRestClient.getLatestIoMsg("OUT")).thenReturn(null);

        KafkaLatencyResponse response = service.calculateReceiveFromKafkaLatency();

        assertAll(
                () -> assertEquals("NO_CHANGER_DATA", response.status()),
                () -> assertNull(response.latencyMs())
        );
    }

    @Test
    @DisplayName("расчет времени получения из kafka когда адаптер еще не получил ответ")
    void testCalculateReceiveFromKafkaLatencyNotReceivedYet() {
        ZonedDateTime sentAt = ZonedDateTime.now();
        ChangerIoMsgDto changerOut = new ChangerIoMsgDto(1L, 10L, "req-pending", "KAFKA", "GRU", "OUT", "161", sentAt);
        when(changerRestClient.getLatestIoMsg("OUT")).thenReturn(changerOut);
        when(ioMsgsDao.findByRequestIdAndDir("req-pending", Dir.IN)).thenReturn(null);

        KafkaLatencyResponse response = service.calculateReceiveFromKafkaLatency();

        assertAll(
                () -> assertEquals("NOT_RECEIVED_BY_ADAPTER_YET", response.status()),
                () -> assertEquals("req-pending", response.requestId()),
                () -> assertNull(response.latencyMs())
        );
    }

    @Test
    @DisplayName("успешный расчет времени получения записи ченджером от продюсера")
    void testCalculateReceiveFromProducerLatencyOk() {
        ZoneId zone = ZoneId.of("Europe/Moscow");
        ZonedDateTime sentAt = ZonedDateTime.of(2026, 11, 18, 12, 0, 0, 0, zone);
        ZonedDateTime receivedAt = sentAt.plusNanos(35_000_000);
        ChangerIoMsgDto changerIn = new ChangerIoMsgDto(2L, 20L, "req-2", "KAFKA", "GRU", "IN", "162", receivedAt);
        AppAdapterIoMsgs adapterOut = new AppAdapterIoMsgs().setInsTs(sentAt.toLocalDateTime());

        when(changerRestClient.getLatestIoMsg("IN")).thenReturn(changerIn);
        when(ioMsgsDao.findByRequestIdAndDir("req-2", Dir.OUT)).thenReturn(adapterOut);

        KafkaLatencyResponse response = service.calculateReceiveFromProducerLatency();

        assertAll(
                () -> assertEquals("OK", response.status()),
                () -> assertEquals("req-2", response.requestId()),
                () -> assertEquals(35L, response.latencyMs())
        );
    }

    @Test
    @DisplayName("расчет времени от продюсера при отсутствии данных ченджера")
    void testCalculateReceiveFromProducerLatencyNoChangerData() {
        when(changerRestClient.getLatestIoMsg("IN")).thenReturn(null);

        KafkaLatencyResponse response = service.calculateReceiveFromProducerLatency();

        assertAll(
                () -> assertEquals("NO_CHANGER_DATA", response.status()),
                () -> assertNull(response.latencyMs())
        );
    }

    @Test
    @DisplayName("расчет времени от продюсера при отсутствии записи в адаптере")
    void testCalculateReceiveFromProducerLatencyNoAdapterData() {
        ZonedDateTime receivedAt = ZonedDateTime.now();
        ChangerIoMsgDto changerIn = new ChangerIoMsgDto(2L, 20L, "req-orphan", "KAFKA", "GRU", "IN", "162", receivedAt);
        when(changerRestClient.getLatestIoMsg("IN")).thenReturn(changerIn);
        when(ioMsgsDao.findByRequestIdAndDir("req-orphan", Dir.OUT)).thenReturn(null);

        KafkaLatencyResponse response = service.calculateReceiveFromProducerLatency();

        assertAll(
                () -> assertEquals("NO_ADAPTER_DATA", response.status()),
                () -> assertEquals("req-orphan", response.requestId()),
                () -> assertNull(response.latencyMs())
        );
    }
}
