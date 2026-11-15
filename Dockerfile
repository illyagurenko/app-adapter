FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

RUN mkdir -p /app/logs

COPY build/libs/*.jar app.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "app.jar"]

