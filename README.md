
# App-adapter Service

Сервис пакетной вычитки финансовых записей из `GRU.GRU_VISTA_TAB`, публикации батчей в Apache Kafka и обработки входящих ответов с фиксацией результатов в Oracle.

---

## Назначение

`app-adapter` выполняет роль интеграционного шлюза между учетной системой Oracle и внешним процессингом (SMART / Kafka):

1. **Producer-контур:** вычитывает порции записей (батчи по 100 шт.) со статусом `WAIT`, переводит их в `IN_PROCESS`, формирует транзакцию `APP_ADAPTER_TRANS`, упаковывает события в JSON и публикует в Kafka-топик запросов.
2. **Consumer-контур:** асинхронно считывает ответы из Kafka-топика ответов, проверяет целостность пачки, обновляет остатки (`OLDTBAL`, `NEWTBAL`) и статус в `GRU_VISTA_TAB`, а ошибочные записи регистрирует в таблице брака `GRU_REJECT_TAB`.
3. **Recovery-контур:** периодически сбрасывает зависшие в `IN_PROCESS` записи обратно в `WAIT` при сбоях сети или инстансов.

### Схема движения данных

```text
               [ ORACLE: GRU.GRU_VISTA_TAB ]
                             │
                      (FOC_STATUS = WAIT)
                             │
                             ▼
              [ GruBatchingService (FOR UPDATE) ]
                             │
                  (FOC_STATUS = IN_PROCESS)
                             │
                             ▼
                    [ GruProducerWorker ]
                             │
              (Запись в APP_ADAPTER_TRANS: CONSUMER_WAIT)
              (Запись в APP_ADAPTER_IO_MSGS: DIR = OUT)
                             │
                             ▼
              [ Kafka: gru.balance.request ]
                             │
                             ▼
                   Внешний процессинг (SMART)
                             │
                             ▼
              [ Kafka: gru.balance.response ]
                             │
                             ▼
                   [ AppKafkaConsumer ]
                             │
                    [ GruConsumerService ]
                             │
              (Запись в APP_ADAPTER_TRANS: CONSUMER_IN_PROCESS)
              (Запись в APP_ADAPTER_IO_MSGS: DIR = IN)
                             │
                             ▼
                   [ GruValidateService ]
                 /                        \
                ▼                          ▼
      (Успешные строки)             (Ошибки / Сбой пачки)
      FOC_STATUS = SUCCESS          FOC_STATUS = ERROR
      Обновление OLDTBAL/NEWTBAL    Запись в GRU_REJECT_TAB
```

---

## Стек технологий

- **Java:** 21 (Records, Pattern Matching)
- **Фреймворк:** Spring Boot 3.3.5
- **База данных:** Oracle Database (XE / Enterprise)
- **ORM / Data Access:** MyBatis 3 (вынесенный SQL в XML-мапперах)
- **Брокер сообщений:** Apache Kafka (`spring-kafka`)
- **Миграции БД:** Liquibase
- **Мониторинг:** Spring Boot Actuator, Micrometer
- **Сборщик:** Gradle

---

## Структура пакетов проекта

```text
ru.itone.illya4gurenko/
├── AppAdapterRun.java                      # главный класс запуска и регистрация shutdown hook
├── config/                                 # конфигурации и управление жизненным циклом
│   ├── AppInitializer.java                 # динамический старт/стоп воркеров по данным из бд
│   └── KafkaConfig.java                    # настройки продюсера и консьюмера kafka
├── controller/                             # административное rest api
│   ├── AdminController.java                # эндпоинты статуса, перезапуска и выключения
│   └── dto/
│       └── AdminResponse.java              # dto ответа состояния адаптера
├── dao/                                    # слой изоляции доступа к данным (dao)
│   ├── AppAdapterConfigDao.java
│   ├── AppAdapterIoMsgsDao.java
│   ├── AppAdapterTransDao.java
│   ├── GruRejectDao.java
│   └── GruVistaDao.java
├── dto/                                    # dto модели обмена через kafka
│   ├── ConsumerEventDto.java               # элемент массива событий в ответе
│   ├── ConsumerKafkaDto.java               # корневое входящее сообщение
│   ├── ErrorDto.java                       # описание ошибки (код + сообщение)
│   ├── EventDataDto.java                   # данные балансов (oldTbal, newTbal)
│   ├── ProducerEventDto.java               # элемент массива событий в запросе
│   └── ProducerKafkaDto.java               # корневое исходящее сообщение
├── entity/                                 # сущности таблиц бд
│   ├── AppAdapterConfig.java
│   ├── AppAdapterIoMsgs.java
│   ├── AppAdapterTrans.java
│   ├── GruRejectTab.java
│   ├── GruVistaTab.java
│   └── enums/                              # перечисления бизнес- и системных статусов
│       ├── Dir.java                        # IN / OUT
│       ├── EntityType.java                 # CARD / ACCOUNT
│       ├── EventType.java                  # BALANCE / FOOD
│       ├── FocStatus.java                  # WAIT, IN_PROCESS, CONSUMER_WAIT, CONSUMER_IN_PROCESS, SUCCESS, ERROR
│       ├── MsgType.java                    # GRU
│       ├── ProcType.java                   # IMMEDIATE / INTIME
│       └── Type.java                       # DR / CR / ZR
├── monitor/                                # сервисы метрик
│   └── KafkaBufferMonitor.java             # шедулер мониторинга буфера kafka producer
├── repository/                             # mybatis mapper-интерфейсы
│   ├── AppAdapterConfigRepository.java
│   ├── AppAdapterIoMsgsRepository.java
│   ├── AppAdapterTransRepository.java
│   ├── GruRejectTabRepository.java
│   └── GruVistaTabRepository.java
├── service/                                # бизнес-логика
│   ├── consumer/                           # обработка ответов из kafka
│   │   ├── AppKafkaConsumer.java           # универсальный слушатель контейнера kafka
│   │   ├── ConsumerStatusService.java      # транзакционное управление статусами консьюмера
│   │   ├── GruConsumerService.java         # координатор парсинга и вызова валидации
│   │   ├── GruValidateService.java         # сверка пачки, обновление балансов и брак в reject
│   │   └── MessageConsumer.java            # интерфейс потребителя сообщений
│   ├── producer/                           # подготовка и публикация запросов
│   │   ├── GruBatchingService.java         # блокировка и выгрузка пачки (FOR UPDATE SKIP LOCKED) с нарезкой по 100
│   │   ├── GruProducerService.java         # координатор упаковки и вызова отправки
│   │   ├── KafkaProducerSender.java        # синхронная отправка в kafka template
│   │   └── ProducerStatusService.java      # транзакционное управление статусами продюсера
├── utils/                                  # утилитарные компоненты
│   ├── AdapterEntityFactory.java           # фабрика маппинга сущностей и dto
│   ├── MapperUtils.java                    # json сериализация / десериализация (jackson)
│   └── TimeAndSizeUtils.java               # конвертер единиц времени и памяти
└── worker/                                 # рабочие потоки (runnable)
    └── GruProducerWorker.java              # цикл опроса бд и засыпания продюсера
```

---

## Жизненный цикл статусов

### 1. Таблица `GRU.GRU_VISTA_TAB` (бизнес-записи)
* `WAIT` — запись ожидает отправки. Учитывается поле `FOC_TYPE`:
    * `IMMEDIATE` — готова к отправке сразу;
    * `INTIME` — готова только при наступлении времени `FOC_TS <= SYSDATE`.
* `IN_PROCESS` — пачка заблокирована воркером и отправляется в Kafka.
* `SUCCESS` — из Kafka получен успешный ответ, обновлены `OLDTBAL` и `NEWTBAL`.
* `ERROR` — запись отклонена внешней системой или нарушена целостность пачки (запись дублируется в `GRU_REJECT_TAB`).

### 2. Таблица `ACC_APP_ADAPTER.APP_ADAPTER_TRANS` (транзакции батчей)
* `CONSUMER_WAIT` — пачка успешно опубликована в топик запросов `gru.balance.request`, ожидается ответ.
* `CONSUMER_IN_PROCESS` — ответ из топика `gru.balance.response` получен консьюмером и взят в обработку.
* `SUCCESS` — ответ успешно провалидирован, бизнес-данные обновлены.
* `ERROR` — техническая ошибка сериализации, сбой отправки в Kafka или отклонение внешней системой.

---

## Форматы Kafka DTO

### Исходящее сообщение (`gru.balance.request`)

```json
{
  "actualTimestamp": 1729000000000,
  "systemId": "GRU",
  "requestId": "d8f07b46-0b1a-4d2a-8742-56453965b123",
  "eventType": "BALANCE",
  "entityType": "ACCOUNT",
  "events": [
    {
      "id": 10001,
      "systemAccount": "40817810000000001234",
      "currency": "RUB",
      "xalfa": 500.00,
      "operation": "CR"
    }
  ]
}
```

### Входящее сообщение (`gru.balance.response`)

**Успешный ответ (или частичный отказ строк):**
```json
{
  "actualTimestamp": 1729000001000,
  "systemId": "SMART",
  "requestId": "d8f07b46-0b1a-4d2a-8742-56453965b123",
  "eventType": "BALANCE",
  "entityType": "ACCOUNT",
  "status": "SUCCESS",
  "events": [
    {
      "entityValue": "40817810000000001234",
      "entityId": 10001,
      "data": {
        "oldTbal": 1500.00,
        "newTbal": 2000.00,
        "operation": "CR"
      },
      "status": "SUCCESS"
    },
    {
      "entityValue": "40817810000000005678",
      "entityId": 10002,
      "status": "ERROR",
      "error": {
        "code": "ERR-042",
        "message": "Account is blocked"
      }
    }
  ]
}
```

**Полный отказ пачки:**
```json
{
  "actualTimestamp": 1729000001000,
  "systemId": "SMART",
  "requestId": "d8f07b46-0b1a-4d2a-8742-56453965b123",
  "eventType": "BALANCE",
  "entityType": "ACCOUNT",
  "status": "ERROR",
  "error": {
    "code": "FATAL-001",
    "message": "System unavailable"
  }
}
```

> **Важно:** если в ответе возвращены не все `entityId`, которые отправлялись в батче, адаптер признает пачку неконсистентной и переводит **все** записи батча в статус `ERROR` с занесением в `GRU_REJECT_TAB`.

---

## Конфигурация приложения (`application.yaml`)

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

---

## База данных и Liquibase

### Управление схемами
* Схема `ACC_APP_ADAPTER` — техническая схема адаптера (управляется через Liquibase проекта):
    * `APP_ADAPTER_CONFIG` — параметры топиков и флаги активности потоков (`STATUS_IN`, `STATUS_OUT`).
    * `APP_ADAPTER_TRANS` — журнал отправленных пакетов и их статусов.
    * `APP_ADAPTER_IO_MSGS` — аудит сырых входящих и исходящих JSON-сообщений с привязкой к ноде (`pc.number`).
* Схема `GRU` — бизнес-схема (внешняя):
    * `GRU.GRU_VISTA_TAB` — очередь финансовых проводок к обработке.
    * `GRU.GRU_REJECT_TAB` — реестр отклоненных проводок.

### Инициализация конфигурации (`APP_ADAPTER_CONFIG`)

При накате Liquibase создается начальная запись для системы `GRU`:

```sql
SELECT SYSTEMID, TOPIC_IN, STATUS_IN, TOPIC_OUT, STATUS_OUT 
FROM ACC_APP_ADAPTER.APP_ADAPTER_CONFIG 
WHERE SYSTEMID = 'GRU';
```

Значения:
* `SYSTEMID` = `'GRU'`
* `TOPIC_IN` = `'gru.balance.response'`
* `STATUS_IN` = `1` (консьюмер включен)
* `TOPIC_OUT` = `'gru.balance.request'`
* `STATUS_OUT` = `1` (продюсер включен)

---

## Административное REST API

Базовый путь: `http://localhost:8082/api/admin`

| Метод | Путь | Описание |
| :--- | :--- | :--- |
| `GET` | `/api/admin/status` | Получить состояние адаптера (активность, число продюсеров и консьюмеров) |
| `POST` | `/api/admin/workers/stop` | Принудительно остановить рабочие потоки Kafka без остановки REST-сервиса |
| `POST` | `/api/admin/restart` | Перечитать `APP_ADAPTER_CONFIG` из БД и пересоздать все воркеры |
| `POST` | `/api/admin/shutdown` | Запустить процесс безопасного завершения JVM (код 202 Accepted) |

---

## Запуск и развертывание

### 1. Запуск в Docker Compose (2 экземпляра)

Сервис упакован в классический Dockerfile (`eclipse-temurin:21-jre-jammy`) и разворачивается в двух параллельных экземплярах для высокой доступности и параллельной обработки очереди Oracle через `FOR UPDATE SKIP LOCKED`.

Контейнеры подключены к общей внешней сети `food_bridge`.

```bash
# Сборка JAR и запуск двух инстансов
gradlew build -x test
docker compose up -d
```

Инстансы и порты:
* **`app_adapter_1`:** порт `8082`, `PC_NUMBER: 161`, логи в `./logs/gru-adapter-161`
* **`app_adapter_2`:** порт `8083`, `PC_NUMBER: 162`, логи в `./logs/gru-adapter-162`

Проверка статуса:
```bash
curl http://localhost:8082/api/admin/status
curl http://localhost:8083/api/admin/status
```

---

### 2. Локальный запуск через Gradle

```bash
# Сборка jar без тестов
gradlew clean build -x test

# Запуск приложения
gradlew bootRun
```

---

## Тестирование

Проект содержит модульные и интеграционные тесты с соблюдением требований к покрытию кода (Jacoco >= 80%) и краткими названиями `@DisplayName` в нижнем регистре.

* **Модульные тесты (`src/test/java`):** изолированное тестирование DAO, сервисов валидации, продюсера и консьюмера с Mockito/H2.
* **Интеграционные тесты (`src/IntegrationTest/java`):** 
  * Проверка успешного поднятия Spring-контекста.
  * Тест работы продюсера (вычитка из БД и отправка в Kafka).
  * Тест консьюмера с проверкой позитивного и негативного сценариев (успешная финализация и брак).

```bash
# Запуск модульных тестов с отчетом Jacoco
gradlew test

# Запуск интеграционных тестов
gradlew integrationTest
```

---

## Запросы для мониторинга и замера скорости

### Замер времени и скорости обработки по `GRU_VISTA_TAB`:
```sql
SELECT 
    COUNT(*)                                                               AS PROCESSED_ROWS,
    TO_CHAR(MIN(FOC_TS), 'YYYY-MM-DD HH24:MI:SS')                          AS START_TIME,
    TO_CHAR(MAX(FOC_TS), 'YYYY-MM-DD HH24:MI:SS')                          AS END_TIME,
    ROUND((MAX(FOC_TS) - MIN(FOC_TS)) * 86400, 1)                          AS DURATION_SEC,
    ROUND((MAX(FOC_TS) - MIN(FOC_TS)) * 1440, 2)                           AS DURATION_MIN,
    ROUND(COUNT(*) / NULLIF((MAX(FOC_TS) - MIN(FOC_TS)) * 86400, 0), 1)   AS ROWS_PER_SEC
FROM GRU.GRU_VISTA_TAB
WHERE FOC_STATUS IN ('SUCCESS', 'ERROR');
```

### Сводка по статусам в реальном времени:
```sql
SELECT 
    COUNT(*)                                              AS TOTAL_ROWS,
    COUNT(CASE WHEN FOC_STATUS = 'WAIT' THEN 1 END)       AS IN_WAIT,
    COUNT(CASE WHEN FOC_STATUS = 'IN_PROCESS' THEN 1 END) AS IN_PROCESS,
    COUNT(CASE WHEN FOC_STATUS = 'SUCCESS' THEN 1 END)    AS SUCCESS,
    COUNT(CASE WHEN FOC_STATUS = 'ERROR' THEN 1 END)      AS ERROR
FROM GRU.GRU_VISTA_TAB;
```

---

## Безопасность (Security Notice)

Эндпоинты `/api/admin/**` управляют состоянием процессов и завершением JVM. В промышленной среде они **должны быть защищены** сетевыми политиками (Firewall / Ingress), либо закрыты с помощью Spring Security (Basic / OAuth2).
