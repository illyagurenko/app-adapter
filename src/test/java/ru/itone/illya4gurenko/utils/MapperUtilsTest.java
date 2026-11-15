package ru.itone.illya4gurenko.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;
import ru.itone.illya4gurenko.entity.enums.MsgType;

import static org.junit.jupiter.api.Assertions.*;

class MapperUtilsTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MapperUtils mapperUtils = new MapperUtils(objectMapper);

    @Test
    @DisplayName("сериализация объекта в json строку")
    void testTryMappingToStrSuccess() {
        ProducerKafkaDto dto = new ProducerKafkaDto().setSystemId(MsgType.GRU).setRequestId("r1");
        String json = mapperUtils.tryMappingToStr(dto);
        assertTrue(json.contains("r1"));
    }

    @Test
    @DisplayName("возврат пустой строки при ошибке сериализации в json")
    void testTryMappingToStrError() {
        ObjectMapper failingMapper = org.mockito.Mockito.mock(ObjectMapper.class);
        try {
            org.mockito.Mockito.when(failingMapper.writeValueAsString(org.mockito.ArgumentMatchers.any()))
                    .thenThrow(new RuntimeException("Serialization failure"));
        } catch (Exception ignored) {}
        MapperUtils utils = new MapperUtils(failingMapper);
        assertEquals("", utils.tryMappingToStr(new ProducerKafkaDto()));
    }

    @Test
    @DisplayName("успешная десериализация в ConsumerKafkaDto")
    void testTryMappingToConsumerKafkaDtoSuccess() {
        String json = "{\"requestId\":\"req-123\",\"status\":\"SUCCESS\"}";
        ConsumerKafkaDto dto = mapperUtils.tryMappingToConsumerKafkaDto(json);
        assertEquals("req-123", dto.getRequestId());
    }

    @Test
    @DisplayName("возврат null при некорректном json для ConsumerKafkaDto")
    void testTryMappingToConsumerKafkaDtoInvalidJson() {
        ConsumerKafkaDto dto = mapperUtils.tryMappingToConsumerKafkaDto("invalid-json");
        assertNull(dto);
    }

    @Test
    @DisplayName("успешная десериализация в ProducerKafkaDto")
    void testTryMappingToProducerKafkaDtoSuccess() {
        String json = "{\"requestId\":\"req-prod\",\"systemId\":\"GRU\"}";
        ProducerKafkaDto dto = mapperUtils.tryMappingToProducerKafkaDto(json);
        assertEquals("req-prod", dto.getRequestId());
    }

    @Test
    @DisplayName("возврат null при некорректном json для ProducerKafkaDto")
    void testTryMappingToProducerKafkaDtoInvalidJson() {
        ProducerKafkaDto dto = mapperUtils.tryMappingToProducerKafkaDto("invalid-json");
        assertNull(dto);
    }
}

