# App-adapter Service

Сервис обработки записей `GRU.GRU_VISTA_TAB` и интеграции с Kafka.

## Назначение

`app-adapter` читает готовые к обработке записи из Oracle, объединяет их в batch, формирует Kafka request и публикует его во внешний контур.

Ответ из Kafka обрабатывается consumer-ом и используется для обновления статусов и балансов в Oracle.

Основной поток:

```text
GRU.GRU_VISTA_TAB
        ↓
      WAIT
        ↓
App-adapter producer
        ↓
    IN_PROCESS
        ↓
gru.balance.request
        ↓
внешний обработчик / mock
        ↓
gru.balance.response
        ↓
App-adapter consumer
        ↓
SUCCESS / ERROR
```

## Стек

- Java 21
- Spring Boot 3.3.5
- MyBatis
- Oracle
- Spring Kafka
- Liquibase
- Spring Boot Actuator
- Gradle

## Порт

```text
8082
```

## Oracle

Пользователь:

```text
ACC_APP_ADAPTER
```

Пароль локального тестового контура:

```text
root
```

Локальный JDBC URL:

```text
jdbc:oracle:thin:@//localhost:1521/XE
```

Основные схемы:

```text
GRU
ACC_APP_ADAPTER
```

Adapter читает бизнес-данные из:

```text
GRU.GRU_VISTA_TAB
```

и хранит технические данные в:

```text
ACC_APP_ADAPTER.APP_ADAPTER_CONFIG
ACC_APP_ADAPTER.APP_ADAPTER_TRANS
ACC_APP_ADAPTER.APP_ADAPTER_IO_MSGS
```

## APP_ADAPTER_CONFIG

Конфигурация потоков читается из:

```text
ACC_APP_ADAPTER.APP_ADAPTER_CONFIG
```

Для `GRU` должно быть:

```text
SYSTEMID    = GRU
EVENT_TYPE  = BALANCE
ENTITY_TYPE = ACCOUNT

TOPIC_IN    = gru.balance.response
STATUS_IN   = 1

TOPIC_OUT   = gru.balance.request
STATUS_OUT  = 1
```

Проверка:

```sql
SELECT
    SYSTEMID,
    TOPIC_IN,
    STATUS_IN,
    TOPIC_OUT,
    STATUS_OUT
FROM ACC_APP_ADAPTER.APP_ADAPTER_CONFIG
WHERE SYSTEMID = 'GRU';
```

## Kafka

Локальный кластер:

```text
localhost:9092
localhost:9093
localhost:9094
```

Топики:

```text
gru.balance.request
gru.balance.response
```

Направление:

```text
Adapter producer
    ↓
gru.balance.request

gru.balance.response
    ↓
Adapter consumer
```

### Создание топиков

Топики создаются вручную, а не через application YAML.

Request:

```cmd
docker exec kafka1 kafka-topics ^
  --bootstrap-server kafka1:29092 ^
  --create ^
  --topic gru.balance.request ^
  --partitions 3 ^
  --replication-factor 3
```

Response:

```cmd
docker exec kafka1 kafka-topics ^
  --bootstrap-server kafka1:29092 ^
  --create ^
  --topic gru.balance.response ^
  --partitions 3 ^
  --replication-factor 3
```

Проверка:

```cmd
docker exec kafka1 kafka-topics ^
  --bootstrap-server kafka1:29092 ^
  --describe ^
  --topic gru.balance.request
```

Ожидается:

```text
PartitionCount: 3
ReplicationFactor: 3
```

## `application.yaml`

Пример локального контура:

```yaml
server:
  port: 8082
  shutdown: graceful

spring:
  application:
    name: app-adapter

  datasource:
    url: jdbc:oracle:thin:@//localhost:1521/XE
    username: acc_app_adapter
    password: root
    driver-class-name: oracle.jdbc.OracleDriver

  kafka:
    bootstrap-servers: localhost:9092,localhost:9093,localhost:9094

    consumer:
      group-id: my-consumer-group

  lifecycle:
    timeout-per-shutdown-phase: 15s

mybatis:
  config-location: classpath:mybatis-config.xml

batch:
  size: 100

  timeout:
    worker:
      delay: 10000

app:
  adapter:
    producer:
      pool-size: 1

    consumer:
      pool-size: 1

pc:
  number: 162

logging:
  config: classpath:file-logback.xml

dynamic:
  log:
    path: ./logs/gru-adapter
```

## MyBatis

Конфигурация остаётся в XML.

Структура:

```text
src/main/resources/
├── application.yaml
├── mybatis-config.xml
└── mappers/
    ├── app-adapter-config-mapper.xml
    ├── app-adapter-io-msgs-mapper.xml
    ├── app-adapter-trans-mapper.xml
    ├── gru-reject-tab-mapper.xml
    └── gru-vista-tab-mapper.xml
```

`mybatis-config.xml`:

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE configuration
        PUBLIC "-//mybatis.org//DTD Config 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-config.dtd">

<configuration>

    <settings>
        <setting name="mapUnderscoreToCamelCase" value="true"/>
        <setting name="defaultEnumTypeHandler"
                 value="org.apache.ibatis.type.EnumTypeHandler"/>
        <setting name="jdbcTypeForNull" value="NULL"/>
    </settings>

    <typeAliases>
        <package name="ru.itone.illya4gurenko.entity"/>
    </typeAliases>

    <mappers>
        <mapper resource="mappers/app-adapter-config-mapper.xml"/>
        <mapper resource="mappers/app-adapter-io-msgs-mapper.xml"/>
        <mapper resource="mappers/app-adapter-trans-mapper.xml"/>
        <mapper resource="mappers/gru-reject-tab-mapper.xml"/>
        <mapper resource="mappers/gru-vista-tab-mapper.xml"/>
    </mappers>

</configuration>
```

## Выбор batch

Adapter должен выбирать только записи:

```text
FOC_STATUS = WAIT
```

и учитывать тип обработки:

```text
IMMEDIATE
```

или:

```text
INTIME AND FOC_TS <= SYSDATE
```

Пример SQL:

```sql
SELECT *
FROM GRU.GRU_VISTA_TAB
WHERE ID IN (
    SELECT ID
    FROM (
        SELECT ID
        FROM GRU.GRU_VISTA_TAB
        WHERE FOC_STATUS = 'WAIT'
          AND (
              FOC_TYPE = 'IMMEDIATE'
              OR (
                  FOC_TYPE = 'INTIME'
                  AND FOC_TS <= SYSDATE
              )
          )
        ORDER BY ID ASC
    )
    WHERE ROWNUM <= :limit
)
FOR UPDATE SKIP LOCKED;
```

Размер batch:

```yaml
batch:
  size: 100
```

## Worker lifecycle

При старте `AppInitializer`:

1. вызывает `APP_ADAPTER_CONFIG`;
2. получает конфигурацию `GRU`;
3. при `STATUS_OUT = 1` создаёт producer worker-ы;
4. при `STATUS_IN = 1` создаёт consumer worker-ы.

Producer worker-ы запускаются через `ExecutorService`.

Consumer worker-ы создают `KafkaMessageListenerContainer`.

## Admin REST API

Базовый URL:

```text
http://localhost:8082/api/admin
```

### Получить состояние

```http
GET /api/admin/status
```

Пример ответа:

```json
{
  "message": "Current app-adapter state",
  "running": true,
  "producers": 1,
  "consumers": 1
}
```

`running=true` означает, что worker-ы были созданы и не были остановлены.

Это **не является Kafka health-check**. Kafka может быть недоступна, а worker-ы при этом продолжают существовать и пытаться reconnect.

### Остановить worker-ы

```http
POST /api/admin/workers/stop
```

Пример:

```json
{
  "message": "All workers stopped",
  "running": false,
  "producers": 0,
  "consumers": 0
}
```

При этом Spring Boot и REST API продолжают работать.

### Перезапустить worker-ы

```http
POST /api/admin/restart
```

Последовательность:

```text
stop current workers
        ↓
повторный SELECT APP_ADAPTER_CONFIG
        ↓
создание новых producer workers
        ↓
создание новых consumer workers
```

Пример ответа:

```json
{
  "message": "APP_ADAPTER_CONFIG reloaded and workers restarted",
  "running": true,
  "producers": 1,
  "consumers": 1
}
```

### Полностью выключить сервис

```http
POST /api/admin/shutdown
```

Endpoint возвращает:

```text
202 Accepted
```

после чего вызывает завершение JVM.

## Shutdown hook / SIGTERM

В `AppAdapterRun` регистрируется:

```java
Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
            log.info("Shutdown hook started");

            try {
                appInitializer.stopWorkers();
            } catch (Exception e) {
                log.error("Error during shutdown", e);
            }

            log.info("Shutdown hook finished");
        }, "app-adapter-shutdown-hook")
);
```

Hook регистрируется **один раз при запуске приложения**.

В REST endpoint `/shutdown` повторно регистрировать hook не нужно.

Цепочка REST shutdown:

```text
POST /api/admin/shutdown
        ↓
System.exit(0)
        ↓
JVM shutdown
        ↓
shutdown hook
        ↓
stopWorkers()
```

Цепочка Linux:

```text
kill -15 <PID>
        ↓
SIGTERM
        ↓
JVM shutdown
        ↓
shutdown hook
        ↓
stopWorkers()
```

Для Docker:

```bash
docker stop app-adapter
```

Docker сначала отправляет процессу `SIGTERM`, поэтому используется тот же shutdown flow.

`@PreDestroy` в `AppInitializer` можно оставить как дополнительный механизм безопасной остановки.

## Liquibase

Adapter владеет только объектами схемы:

```text
ACC_APP_ADAPTER
```

Рекомендуемая структура:

```text
src/main/resources/db/changelog/
├── db.changelog-master.yaml
├── 001-adapter-sequences.sql
├── 002-adapter-tables.sql
├── 003-adapter-constraints-indexes.sql
└── 004-adapter-config.sql
```

Создаются:

```text
APP_ADAPTER_CONFIG
APP_ADAPTER_TRANS
APP_ADAPTER_IO_MSGS

APP_ADAPTER_CONFIG_SEQ
APP_ADAPTER_TRANS_SEQ
APP_ADAPTER_IO_MSGS_SEQ
```

Не изменяйте уже применённый Liquibase changeset после его применения.

Для изменения данных конфигурации создавайте новый changeset.

## Сборка

Windows:

```cmd
gradlew.bat clean build
```

Без тестов:

```cmd
gradlew.bat clean build -x test
```

Запуск:

```cmd
gradlew.bat bootRun
```

## Проверка Kafka

Request topic:

```cmd
docker exec -it kafka1 kafka-console-consumer ^
  --bootstrap-server kafka1:29092 ^
  --topic gru.balance.request ^
  --from-beginning
```

Response topic:

```cmd
docker exec -it kafka1 kafka-console-consumer ^
  --bootstrap-server kafka1:29092 ^
  --topic gru.balance.response ^
  --from-beginning
```

Consumer group:

```cmd
docker exec kafka1 kafka-consumer-groups ^
  --bootstrap-server kafka1:29092 ^
  --describe ^
  --group my-consumer-group
```

## Проверка Admin API в Postman

Порядок:

```text
GET  /api/admin/status
        ↓
POST /api/admin/workers/stop
        ↓
GET  /api/admin/status
        ↓
POST /api/admin/restart
        ↓
GET  /api/admin/status
        ↓
POST /api/admin/shutdown
```

Body для этих endpoint-ов не нужен.

## E2E-тест request-части

1. Поднять Oracle.
2. Поднять Kafka.
3. Запустить `food-card`.
4. Запустить `app-adapter`.
5. Запустить console consumer на `gru.balance.request`.
6. Через generator создать файл.
7. Food-card должен записать строки в `GRU.GRU_VISTA_TAB` со статусом `WAIT`.
8. Adapter должен найти batch и отправить JSON в:

```text
gru.balance.request
```

## Очистка тестовых данных

Очистить VISTA:

```sql
TRUNCATE TABLE GRU.GRU_VISTA_TAB;
```

Посмотреть статусы:

```sql
SELECT FOC_STATUS, COUNT(*)
FROM GRU.GRU_VISTA_TAB
GROUP BY FOC_STATUS;
```

## Поведение Kafka при недоступных брокерах

Kafka producer/consumer по умолчанию выполняют reconnect.

Поэтому при выключенном Kafka-кластере в логах могут повторяться сообщения:

```text
Connection to node ... could not be established
Bootstrap broker ... disconnected
```

Это не означает бесконечный Java loop в admin-коде: reconnect выполняется Kafka client.

Чтобы временно прекратить эти попытки, можно остановить workers:

```http
POST /api/admin/workers/stop
```

После восстановления Kafka:

```http
POST /api/admin/restart
```

## Важный TODO для полного response-flow

Необходимо проверить `GruProducerService`.

Правильная модель состояния транзакции:

```text
создана транзакция
      ↓
IN_PROCESS
      ↓
request успешно отправлен в Kafka
      ↓
остается IN_PROCESS
      ↓
получен response
      ↓
SUCCESS / ERROR
```

Если producer сразу после успешного `KafkaTemplate.send()` выставляет:

```text
SUCCESS
```

то текущий consumer может расценить response как уже обработанный и пропустить его.

Финальный `SUCCESS/ERROR` должен устанавливать consumer после обработки `gru.balance.response`.

## Security

Admin endpoint-ы позволяют:

- останавливать workers;
- перезапускать workers;
- полностью выключать JVM.

В production `/api/admin/**` необходимо закрыть авторизацией и/или сетевыми ограничениями.
