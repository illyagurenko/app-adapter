package ru.itone.illya4gurenko.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import ru.itone.illya4gurenko.controller.dto.ChangerIoMsgDto;

/**
 * rest-клиент для обращения к сервису app-changer за записями APP_CHANGER_IO_MSGS.
 */
@Component
@Slf4j
public class ChangerRestClient {

    private final RestClient restClient;

    public ChangerRestClient(@Value("${changer.url:http://localhost:8080}") String changerUrl) {
        this.restClient = RestClient.builder()
                .baseUrl(changerUrl)
                .build();
    }

    /**
     * запрашивает последнюю запись из APP_CHANGER_IO_MSGS по направлению (IN / OUT).
     */
    public ChangerIoMsgDto getLatestIoMsg(String dir) {
        try {
            return restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/api/io-msgs/latest")
                            .queryParam("dir", dir)
                            .build())
                    .exchange((req, res) -> {
                        if (res.getStatusCode().is2xxSuccessful()) {
                            return res.bodyTo(ChangerIoMsgDto.class);
                        }
                        return null;
                    });
        } catch (Exception e) {
            log.error("Failed to fetch latest IO_MSG (dir={}) from app-changer", dir, e);
            return null;
        }
    }
}
