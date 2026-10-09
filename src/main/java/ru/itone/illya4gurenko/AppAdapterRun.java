package ru.itone.illya4gurenko;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.itone.illya4gurenko.сonfig.AppInitializer;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class AppAdapterRun {

    public static void main(String[] args) {

        ConfigurableApplicationContext context =
                SpringApplication.run(AppAdapterRun.class, args);

        AppInitializer appInitializer =
                context.getBean(AppInitializer.class);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {

                    log.info("shutdown hook started");

                    try {
                        appInitializer.stopWorkers();

                        log.info("producer and consumer workers stopped");

                    } catch (Exception e) {
                        log.error("error during shutdown", e);
                    }

                    log.info("shutdown hook finished");

                }, "app-adapter-shutdown-hook")
        );

        log.info("shutdown hook registered");
    }
}