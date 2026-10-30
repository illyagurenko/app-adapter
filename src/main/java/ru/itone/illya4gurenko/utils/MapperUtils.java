package ru.itone.illya4gurenko.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.itone.illya4gurenko.dto.ConsumerKafkaDto;
import ru.itone.illya4gurenko.dto.ProducerKafkaDto;

/**
 * утилитарный класс для преобразования java-объектов (dto) в строку json и обратно.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MapperUtils {

    private final ObjectMapper objectMapper;

    /**
     * преобразует объект ProducerKafkaDto в json-строку для отправки в кафку.
     */
    public String tryMappingToStr(ProducerKafkaDto curDto){
        String json;
        try {
            json = objectMapper.writeValueAsString(curDto);
            log.debug("success mapping\n{}", json); // ИСПРАВЛЕНО: error заменен на debug
            return json;
        } catch (Exception e) {
            log.error("error mapping", e);
            return "";
        }
    }

    /**
     * преобразует полученную из кафки json-строку в объект ConsumerKafkaDto.
     */
    public ConsumerKafkaDto tryMappingToConsumerKafkaDto(String json){
        ConsumerKafkaDto consumerKafkaDto;
        try {
            consumerKafkaDto = objectMapper.readValue(json, ConsumerKafkaDto.class);
            return consumerKafkaDto;
        } catch (Exception e) {
            log.error("error parse json: {}", json, e);
            return null;
        }
    }

    /**
     * преобразует сохраненную в бд json-строку обратно в объект ProducerKafkaDto (для сверки).
     */
    public ProducerKafkaDto tryMappingToProducerKafkaDto(String json){
        ProducerKafkaDto sentDto;
        try {
            sentDto = objectMapper.readValue(json, ProducerKafkaDto.class);
            return sentDto;
        } catch (Exception e) {
            // ИСПРАВЛЕНО: добавлен проброс 'e' для отображения стектрейса
            log.error("error parse sent trans.data", e);
            return null;
        }
    }

}