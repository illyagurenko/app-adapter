# Архитектурная и эксплуатационная документация: Контур процессинга реестров (Generator → Food-Card → App-Adapter)

Настоящий документ содержит полное описание архитектуры, контрактов взаимодействия, моделей данных, конфигураций развертывания и эксплуатационных процедур распределенного интеграционного комплекса из трех сервисов: **`generator-service`**, **`food-card`** и **`app-adapter`**.

---

## 1. Общая архитектура и сквозной бизнес-пайплайн (E2E)

Комплекс предназначен для генерации, валидации, аудита, сохранения и последующей передачи в шину Kafka банковских реестров безналичных операций (зачисление, списание, обнуление балансов).

```
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │ 1. GENERATOR SERVICE (Java 21 / Lightweight HTTP / DataFaker)                          │
 │    - Генерация моковых реестров (Fixed-width 152 символа) или прямой batch insert     │
 └──────────────┬───────────────────────────────┬─────────────────────────────┬───────────┘
                │ HTTP Multipart                │ Raw HTTP POST (Chunked)     │ gRPC
                ▼                               ▼                             ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │ 2. FOOD-CARD SERVICE (Spring Boot 3.3.5 / Dual DB: PostgreSQL + Oracle)                │
 │    - Ingestion Gateway, валидация Header, Trailer, Body, контроль дубликатов          │
 │    - Файловая система: in_progress -> (success | error)                                │
 │    - PostgreSQL (схема POM): аудит файлов (file), строк (unit), ошибок (unit_error)    │
 │    - Oracle (схема GRU): вставка мастер-записей в GRU.GRU_VISTA_TAB (FOC_STATUS='WAIT')│
 └──────────────────────────────────────────────┬─────────────────────────────────────────┘
                                                │
                                                │ Shared DB Table: GRU.GRU_VISTA_TAB
                                                ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │ 3. APP-ADAPTER SERVICE (Spring Boot 3.3.5 / MyBatis / Oracle / Spring Kafka)           │
 │    - Producer: SELECT FOR UPDATE SKIP LOCKED -> IN_PROCESS -> gru.balance.request      │
 │    - Технический аудит: ACC_APP_ADAPTER.APP_ADAPTER_TRANS & APP_ADAPTER_IO_MSGS        │
 │    - Consumer: gru.balance.response (Manual Immediate Ack)                            │
 │    - Завершение: GRU_VISTA_TAB (SUCCESS + балансы) ИЛИ GRU_REJECT_TAB (ERROR)          │
 └──────────────────────────────────────────────┬─────────────────────────────────────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │     APACHE KAFKA      │
                                    │ gru.balance.request   │
                                    │ gru.balance.response  │
                                    └───────────────────────┘
```

### Альтернативный контур быстрого тестирования (Short-circuit):
`generator-service` может инициировать генерацию записей напрямую через JDBC в таблицу `GRU.GRU_VISTA_TAB`, минуя файловую передачу и сервис `food-card`.

---

## 2. Детальный разбор сервисов

### 2.1. Сервис генерации тестовых данных (`generator-service`)

* **Назначение:** Формирование тестовой нагрузки, эмуляция автоматизированных банковских систем (АБС), генерация корректных и поврежденных файлов, тестирование пропускной способности каналов доставки.
* **Технологический стек:** Java 21, `com.sun.net.httpserver.HttpServer` (без Spring Boot), Shadow JAR, Net DataFaker, Jackson, Apache HttpClient 5, Java Native HTTP Client, gRPC Stub, Oracle JDBC (`ojdbc11`).
* **Порт:** `8080` (HTTP).

#### Модель доставки файлов:
1. **HTTP Multipart:** Отправка потока через `ApacheMultipartSenderService` (`POST /api/files/upload`).
2. **HTTP Chunked Stream:** Отправка открытого текста через нативный `ChunkedFileSenderService` (`POST /api/files/stream`).
3. **gRPC Stream:** Потоковая передача чанками по 64 КБ через `StreamGrpcSenderService` (`FileUploadServiceGrpc`).
4. **Shared Directory:** Прямая запись в примонтированную общую директорию хоста.

#### Сценарии искажения данных (Негативное тестирование):
Параметр `generator.files.percentage.invalid` (0–100%) активирует генерацию битых файлов по 7 стратегиям:
1. `CORRUPTION_HEADER`: Повреждение строки заголовка.
2. `INVALID_COUNT_RECORDS`: Несовпадение счетчика строк в трейлере с фактическим количеством записей.
3. `CORRUPTION_FOOTER`: Синтаксически неверный подвал.
4. `CORRUPTION_CLIENT_RECORDS`: Битая клиентская строка в середине файла.
5. `INCORRECT_CHARSET`: Генерация в несовместимой кодировке (Windows-1251 или KOI8-R вместо UTF-8).
6. `EMPTY_FILE`: Файл размером 0 байт.
7. `BLANK_FILE`: Файл, состоящий только из пробелов, табуляций и пустых строк.

---

### 2.2. Сервис первичного приема и валидации (`food-card`)

* **Назначение:** Шлюз приема реестров, структурная и логическая валидация, ведение подробного журнала аудита в PostgreSQL, формирование реестра проводок в Oracle.
* **Технологический стек:** Spring Boot 3.3.5, Spring Web, gRPC Server (`net.devh:grpc-server-spring-boot-starter`), Spring Data JPA, Liquibase, PostgreSQL Driver, Oracle JDBC.
* **Порты:** `8081` (HTTP REST), `9090` (gRPC Server).

#### Архитектура хранилищ (Dual-Datasource):
* **PostgreSQL (Схема `pom`):**
    * `pom.file`: Реестр входящих файлов (имя, статус `IN_PROCESS`/`SUCCESS`/`ERROR`, отправитель, юлианская дата, путь на диске).
    * `pom.unit`: Построчный аудит файла (`101` — Заголовок, `106` — Тело, `108` — Трейлер).
    * `pom.unit_error`: Журнал ошибок каждой конкретной строки с кодом ошибки (`H01`, `T01`, `B00`–`B06`) и сообщением.
* **Oracle (Схема `GRU`):**
    * `GRU.GRU_VISTA_TAB`: Бизнес-проводки, готовые к отправке в учетную систему (статус `WAIT`).
    * `GRU.GRU_REJECT_TAB`: Таблица отклоненных транзакций (заполняется адаптером при ошибках из Kafka).

#### Механизм жизненного цикла файла на диске:
1. Файл помещается в `data/YYYYMMDD/in_progress/<filename>.in_progress`.
2. `RandomAccessFile` вычитывает последнюю строку (pre-check трейлера без загрузки файла).
3. Построчная обработка через `Files.lines` и `EnrollVisitor`.
4. При успехе: атомарное перемещение в `data/YYYYMMDD/success/<filename>.success`.
5. При ошибке: перемещение в `data/YYYYMMDD/error/<filename>.error`, удаление вставленных записей в Oracle (`gruVistaTabRepository.deleteByFileId`), выставление статуса `ERROR` в `pom.file`.
6. Шедулер `DataCleanupService` удаляет папки `YYYYMMDD` старше `retention-days` (по умолчанию 3 дня).

---

### 2.3. Сервис интеграции с Kafka (`app-adapter`)

* **Назначение:** Асинхронный адаптер между Oracle и Apache Kafka (паттерн Transactional Outbox / Polling Publisher).
* **Технологический стек:** Spring Boot 3.3.5, MyBatis 3, Spring Kafka, Oracle JDBC, Liquibase, Micrometer Actuator.
* **Порт:** `8082` (HTTP Admin API).

#### Рабочие механизмы:
* **Producer Worker:**
    * Опрашивает `GRU.GRU_VISTA_TAB` пачками через `SELECT ... FOR UPDATE SKIP LOCKED`.
    * Переводит статус записей в `IN_PROCESS`.
    * Создает транзакцию в `ACC_APP_ADAPTER.APP_ADAPTER_TRANS` (`STATUS = 'CONSUMER_WAIT'`).
    * Сохраняет исходящий payload в `ACC_APP_ADAPTER.APP_ADAPTER_IO_MSGS` (`DIR = 'OUT'`).
    * Публикует сообщение в топик `gru.balance.request` с ключом `requestId`.
* **Consumer Worker:**
    * Подписан на топик `gru.balance.response`. Режим подтверждения: `AckMode.MANUAL_IMMEDIATE`.
    * Проверяет транзакцию по `requestId` на идемпотентность (пропуск повторных сообщений).
    * Сохраняет входящий JSON в `APP_ADAPTER_IO_MSGS` (`DIR = 'IN'`).
    * `GruValidateService` сопоставляет отправленные и полученные идентификаторы записей (`sentIds` vs `receivedIds`):
        * При успехе записи: обновление `OLDTBAL`, `NEWTBAL` и статус `SUCCESS` в `GRU.GRU_VISTA_TAB`.
        * При ошибке записи или всей пачки: статус `ERROR` в `GRU_VISTA_TAB` и фиксация записи в `GRU.GRU_REJECT_TAB`.
* **Self-Healing Job (`BadRowsService`):**
    * По расписанию сбрасывает зависшие в `IN_PROCESS` записи обратно в `WAIT`, если ответ от внешней системы не получен в течение заданного таймаута (30 минут).

---

## 3. Спецификация контрактов и форматов взаимодействия

### 3.1. Формат банковского реестра (Fixed-Width)

Имя файла формируется строго по маске:
```text
Z<CodeBank(3)><CodeFilial(3)>.<NameAES>_ENROLL<Number>.<JulianDay(3)>
Пример: Z001002.GPB_ENROLL1.298
```

#### Структура строк:
| Тип строки | Длина | Описание формата | Пример |
|---|:---:|---|---|
| **Header (IMMEDIATE)** | 35 симв. | `H YYYYMMDD HHMMSS IMMEDIATE` | `H 20260330 143000 IMMEDIATE` |
| **Header (INTIME)** | 50 симв. | `H YYYYMMDD HHMMSS INTIME YYYYMMDD HHMMSS` | `H 20260330 143000 INTIME 20260331 100000` |
| **Body (Клиент)** | **152 симв.** | ФИО (100) + Номер счета (30) + Тип (2) + Сумма (20) | См. ниже |
| **Trailer (Подвал)** | 20 симв. | `T` (1) + Пробелы (9) + Количество строк (10) | `T 10` |

#### Детализация позиций Body-строки (152 символа):
```text
Позиция   Длина  Тип       Описание
0..99     100    String    ФИО клиента (выравнивание влево, дополнение пробелами)
100..129  30     String    Номер лицевого счета / карты (выравнивание влево)
130..131  2      String    Тип операции: DR (Дебет), CR (Кредит), ZR (Обнуление)
132..151  20     Decimal   Сумма транзакции (выравнивание вправо, дополнение пробелами)
```

---

### 3.2. Контракт сообщений Apache Kafka

#### Топик запроса: `gru.balance.request`
```json
{
  "actualTimestamp": 1711800000000,
  "systemId": "GRU",
  "requestId": "d8b3c8f2-39c4-4b57-b08e-1f298816c5b1",
  "eventType": "BALANCE",
  "events": [
    {
      "id": 10541,
      "systemAccount": "40817810099910004321",
      "currency": "222",
      "xalfa": 1500.50,
      "operation": "CR"
    }
  ]
}
```

#### Топик ответа: `gru.balance.response`
```json
{
  "actualTimestamp": 1711800005000,
  "systemId": "GRU",
  "requestId": "d8b3c8f2-39c4-4b57-b08e-1f298816c5b1",
  "eventType": "BALANCE",
  "entityType": "ACCOUNT",
  "status": "SUCCESS",
  "error": null,
  "events": [
    {
      "entityValue": "40817810099910004321",
      "entityId": 10541,
      "status": "SUCCESS",
      "error": null,
      "data": {
        "oldTbal": 5000.00,
        "newTbal": 3499.50,
        "operation": "CR"
      }
    }
  ]
}
```

---

## 4. Схемы баз данных и Liquibase миграции

### 4.1. Oracle: Схема `GRU` (Владелец миграций: `food-card`)
* `GRU.GRU_VISTA_TAB`:
    * `ID (NUMBER PK)` — Sequence `GRU_VISTA_SEQ`.
    * `SYSTEMACCOUNT (VARCHAR2 32)` — Номер счета.
    * `XALFA (NUMBER 23,3)` — Сумма операции.
    * `OPERATION (VARCHAR2 2)` — `DR`, `CR`, `ZR`.
    * `POM_ID (NUMBER)` — Связь с `pom.unit.id` в PostgreSQL.
    * `FILE_ID (NUMBER)` — Связь с `pom.file.id`.
    * `FOC_STATUS (VARCHAR2 64)` — `WAIT`, `IN_PROCESS`, `SUCCESS`, `ERROR`.
    * `FOC_TYPE (VARCHAR2 10)` — `IMMEDIATE` или `INTIME`.
    * `FOC_TS (TIMESTAMP)` — Плановое время отложенного исполнения.
    * `FOC_STATUS_TS (TIMESTAMP)` — Время последнего изменения статуса.
* `GRU.GRU_REJECT_TAB`:
    * `ID (NUMBER PK)` — Sequence `GRU_REJECT_SEQ`.
    * `VISTA_TAB_ID (NUMBER 12)` — Ссылка на запись `GRU_VISTA_TAB.ID`.
    * `REJECT_DESC (VARCHAR2 256)` — Описание ошибки из Kafka.
    * `FRONT_STATUS (VARCHAR2 3)` — Код статуса (например, `ERR`).

### 4.2. Oracle: Схема `ACC_APP_ADAPTER` (Владелец миграций: `app-adapter`)
* `APP_ADAPTER_CONFIG`: Настройка топиков и статусов включения воркеров (`SYSTEMID = 'GRU'`, `TOPIC_IN`, `TOPIC_OUT`, `STATUS_IN = 1`, `STATUS_OUT = 1`).
* `APP_ADAPTER_TRANS`: Реестр транзакций запросов (`REQUEST_ID`, `STATUS`, `DATA CLOB`, `RESP_CODE`, `RESP_DESC`).
* `APP_ADAPTER_IO_MSGS`: Аудит сырых входящих и исходящих JSON сообщений (`TRANS_ID`, `DIR = IN/OUT`, `MSG CLOB`, `NODE_ID`).

### 4.3. PostgreSQL: Схема `pom` (Владелец миграций: `food-card`)
* `pom.file`: Журнал обработки файлов (`filename`, `file_status`, `uli_date`, `ins_time`, `upd_time`).
* `pom.unit`: Журнал строк файла (`file_id`, `pom_type`, `unit_value`, `status`, `ins_time`).
* `pom.unit_error`: Список валидационных нарушений по строкам (`unit_id`, `error_code`, `error_msg`).

---

## 5. Выявленные критические дефекты и план исправления (Code Review)

При аудите исходного кода обнаружен ряд дефектов, препятствующих корректной промышленной эксплуатации:

### 1. Критическая ошибка выборки батча в `app-adapter`
* **Файл:** `app-adapter/src/main/resources/mappers/gru-vista-tab-mapper.xml`
* **Проблема:** Запрос `fetchBatchForUpdate` содержит условие `WHERE FOC_STATUS != 'IN_PROCESS'`.
* **Последствие:** Записи со статусом `SUCCESS` и `ERROR` будут повторно выбираться продюсером, образуя бесконечный цикл повторной отправки одних и тех же сообщений в Kafka. Также полностью игнорируется условие `INTIME`.
* **Исправление:** Заменить условие в SQL на строгое соответствие спецификации:
  ```xml
  <select id="fetchBatchForUpdate" resultMap="GruVistaTabMap">
      SELECT ID, SYSTEMACCOUNT, CURRENCY, XALFA, OPERATION, TIME_STAMP,
             POM_ID, UTERRARIO, OLDTBAL, NEWTBAL, ADD_INFO,
             FILE_ID, FOC_STATUS, FOC_TS, FOC_TYPE
      FROM GRU.GRU_VISTA_TAB
      WHERE ID IN (
          SELECT ID FROM (
              SELECT ID FROM GRU.GRU_VISTA_TAB
              WHERE FOC_STATUS = 'WAIT'
                AND (
                    FOC_TYPE = 'IMMEDIATE'
                    OR (FOC_TYPE = 'INTIME' AND FOC_TS &lt;= SYSDATE)
                )
              ORDER BY ID ASC
          ) WHERE ROWNUM &lt;= #{limit}
      )
      FOR UPDATE SKIP LOCKED
  </select>
  ```

### 2. Затирание бизнес-времени `FOC_TS`
* **Файл:** `app-adapter/src/main/resources/mappers/gru-vista-tab-mapper.xml`
* **Проблема:** Метод `updateStatusByIds` выполняет `SET FOC_STATUS = #{newStatus}, FOC_TS = SYSDATE`. Поле `FOC_TS` хранит запланированное время исполнения отложенного платежа. Перезапись его текущим временем уничтожает исходные данные.
* **Исправление:** Обновлять техническое поле времени смены статуса `FOC_STATUS_TS = SYSDATE`, не затрагивая `FOC_TS`.

### 3. Производительность вставки в `food-card` (N+1 Inserts)
* **Файл:** `food-card/.../service/visitor/EnrollVisitor.java`
* **Проблема:** Вызовы `pomDao.saveUnit(...)` и `gruDao.save(...)` выполняются синхронно по одной записи в цикле парсинга. Для реестра на 10 000 строк это приводит к 20 000 сетевым round-trip запросам к БД.
* **Исправление:** Накапливать сущности во внутренний буфер `List` и выполнять пакетное сохранение через JDBC Batch или `saveAll()` пачками по 1000 записей.

### 4. Несоответствие типов параметров в `generator-service`
* **Файл:** `generator-service/.../dto/ParametersRequestDto.java`
* **Проблема:** Поля `codeBank` и `codeFilial` имеют тип `int`. При передаче кодов с ведущими нулями (например, `"044525225"`, `"0001"`) нули теряются.
* **Исправление:** Перевести поля `codeBank` и `codeFilial` на строковый тип `String`.

---

## 6. Руководство по развертыванию и конфигурации

### 6.1. Подготовка сети и пользователей БД
Все сервисы взаимодействуют через единую пользовательскую сеть Docker bridge:
```bash
docker network create food_bridge
```

При первичном развертывании чистого контейнера Oracle необходимо создать учетную запись для адаптера до применения миграций:
```sql
CREATE USER ACC_APP_ADAPTER IDENTIFIED BY root;
ALTER USER ACC_APP_ADAPTER DEFAULT TABLESPACE USERS;
ALTER USER ACC_APP_ADAPTER QUOTA UNLIMITED ON USERS;

GRANT CREATE SESSION TO ACC_APP_ADAPTER;
GRANT CREATE TABLE TO ACC_APP_ADAPTER;
GRANT CREATE SEQUENCE TO ACC_APP_ADAPTER;
```

---

### 6.2. Сводный `docker-compose.yml` полного контура

```yaml
version: "3.8"

services:
  # ==========================================
  # ИНФРАСТРУКТУРА: Базы данных и брокер
  # ==========================================
  postgres:
    image: postgres:16-alpine
    container_name: food_card_postgres
    environment:
      POSTGRES_DB: postgres_db
      POSTGRES_USER: user
      POSTGRES_PASSWORD: root
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d postgres_db"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - food_bridge

  oracle:
    image: gvenzl/oracle-xe:11-slim
    container_name: food_card_oracle
    environment:
      ORACLE_PASSWORD: root
      APP_USER: gru
      APP_USER_PASSWORD: root
    ports:
      - "1521:1521"
    volumes:
      - oracledata:/u01/app/oracle
    healthcheck:
      test: ["CMD-SHELL", "echo 'SELECT 1 FROM DUAL;' | sqlplus -S gru/root@//localhost:1521/XE || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 15
      start_period: 30s
    networks:
      - food_bridge

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: food_card_kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    networks:
      - food_bridge

  # ==========================================
  # БИЗНЕС-СЕРВИСЫ
  # ==========================================
  food-card-app:
    build:
      context: ./food-card
      dockerfile: Dockerfile
    container_name: food-card-app
    ports:
      - "8081:8081"
      - "9090:9090"
    environment:
      SPRING_DATASOURCE_POSTGRES_URL: jdbc:postgresql://postgres:5432/postgres_db
      SPRING_DATASOURCE_POSTGRES_USERNAME: user
      SPRING_DATASOURCE_POSTGRES_PASSWORD: root
      SPRING_DATASOURCE_ORACLE_URL: jdbc:oracle:thin:@//oracle:1521/XE
      SPRING_DATASOURCE_ORACLE_USERNAME: gru
      SPRING_DATASOURCE_ORACLE_PASSWORD: root
      SPRING_FILES_DIR: /app/input_files
      SPRING_FILES_DATA: /app/data
      SPRING_FILES_SCAN_INTERVAL: "5000"
      SPRING_JPA_PROPERTIES_ORACLE_JDBC_TIMEZONEASREGION: "false"
      TZ: Europe/Moscow
    volumes:
      - ./data:/app/data
      - ./shared_exchange:/app/input_files
      - ./config/food-card:/app/config
    depends_on:
      postgres:
        condition: service_healthy
      oracle:
        condition: service_healthy
    networks:
      - food_bridge

  generator-app:
    build:
      context: ./generator-service
      dockerfile: Dockerfile
    container_name: file-generator-service
    ports:
      - "8080:8080"
    volumes:
      - ./config/generator:/app/config
      - ./logs/generator:/app/logs
      - ./shared_exchange:/app/generated
    environment:
      TZ: Europe/Moscow
    command:
      - java
      - -Dgenerator.file.property=/app/config/application.properties
      - -Dgenerator.file.logback=/app/config/file-logback.xml
      - -jar
      - /app/app.jar
    depends_on:
      - food-card-app
    networks:
      - food_bridge

  app-adapter:
    build:
      context: ./app-adapter
      dockerfile: Dockerfile
    container_name: app-adapter
    ports:
      - "8082:8082"
    environment:
      SPRING_DATASOURCE_URL: jdbc:oracle:thin:@//oracle:1521/XE
      SPRING_DATASOURCE_USERNAME: acc_app_adapter
      SPRING_DATASOURCE_PASSWORD: root
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      BATCH_SIZE: "100"
      TZ: Europe/Moscow
    depends_on:
      oracle:
        condition: service_healthy
      kafka:
        condition: service_started
    networks:
      - food_bridge

volumes:
  pgdata:
  oracledata:

networks:
  food_bridge:
    external: true
```

---

## 7. Инструкция по сквозному тестированию (E2E Runbook)

### Шаг 1: Создание топиков Kafka
```bash
docker exec -it food_card_kafka kafka-topics --bootstrap-server localhost:9092 --create --topic gru.balance.request --partitions 3 --replication-factor 1
docker exec -it food_card_kafka kafka-topics --bootstrap-server localhost:9092 --create --topic gru.balance.response --partitions 3 --replication-factor 1
```

### Шаг 2: Запуск мониторинга очереди Kafka
Открыть отдельный терминал для чтения сообщений запроса:
```bash
docker exec -it food_card_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gru.balance.request --from-beginning
```

### Шаг 3: Инициация генерации файлов через Generator
Выполнить HTTP-запрос на генерацию 1 файла на 5 записей:
```http
POST http://localhost:8080/api/parametres
Content-Type: application/json
X-Auth-User: q1
X-Auth-Password: <AES_ENCRYPTED_PASSWORD_OR_RAW>

{
  "codeBank": 445,
  "codeFilial": 1,
  "nameAES": "TEST_AES",
  "countRecords": 5,
  "countFiles": 1,
  "inTime": null
}
```

### Шаг 4: Проверка сохранения проводок в Oracle
```sql
SELECT ID, SYSTEMACCOUNT, XALFA, OPERATION, FOC_STATUS, FOC_TYPE
FROM GRU.GRU_VISTA_TAB
ORDER BY ID DESC;
```
*Ожидаемый результат:* Строки созданы со статусом `WAIT`. После запуска `app-adapter` их статус переходит в `IN_PROCESS`.

### Шаг 5: Проверка технических транзакций в Adapter
```sql
SELECT ID, REQUEST_ID, STATUS, RESP_DESC, INS_TS
FROM ACC_APP_ADAPTER.APP_ADAPTER_TRANS
ORDER BY ID DESC;

SELECT ID, TRANS_ID, DIR, MSG
FROM ACC_APP_ADAPTER.APP_ADAPTER_IO_MSGS
ORDER BY ID DESC;
```
*Ожидаемый результат:* В `APP_ADAPTER_TRANS` зафиксирована транзакция со статусом `CONSUMER_WAIT`, в `APP_ADAPTER_IO_MSGS` сохранена запись с `DIR = 'OUT'`.

### Шаг 6: Эмуляция ответа внешнего сервиса (Mocking Kafka Response)
Отправить ответное сообщение в топик `gru.balance.response` (подставив реальный `requestId` и `entityId` из шага 5):
```bash
docker exec -it food_card_kafka kafka-console-producer --bootstrap-server localhost:9092 --topic gru.balance.response
```
Вставить payload:
```json
{"actualTimestamp":1711800010000,"systemId":"GRU","requestId":"<REQUEST_ID_ИЗ_ШАГА_5>","eventType":"BALANCE","entityType":"ACCOUNT","status":"SUCCESS","events":[{"entityValue":"ACCOUNT_NUMBER","entityId":<ID_ИЗ_ШАГА_4>,"status":"SUCCESS","data":{"oldTbal":1000.00,"newTbal":2500.50,"operation":"CR"}}]}
```

### Шаг 7: Проверка финализации проводок в Oracle
```sql
SELECT ID, OLDTBAL, NEWTBAL, FOC_STATUS
FROM GRU.GRU_VISTA_TAB
WHERE ID = <ID_ИЗ_ШАГА_4>;
```
*Ожидаемый результат:* `FOC_STATUS = 'SUCCESS'`, поля `OLDTBAL` и `NEWTBAL` заполнены значениями из ответа.

---

## 8. Эксплуатация и мониторинг (Admin Operations)

Управление фоновыми воркерами сервиса `app-adapter` выполняется через Admin REST API:

| Метод | URL | Описание |
|---|---|---|
| `GET` | `http://localhost:8082/api/admin/status` | Получить статус воркеров (количество активных продюсеров и консьюмеров). |
| `POST` | `http://localhost:8082/api/admin/workers/stop` | Принудительно остановить чтение БД и прослушивание Kafka (REST API остается доступен). |
| `POST` | `http://localhost:8082/api/admin/restart` | Перечитать таблицу `APP_ADAPTER_CONFIG` и перезапустить воркеры с новыми параметрами. |
| `POST` | `http://localhost:8082/api/admin/shutdown` | Инициировать корректную остановку приложения с вызовом shutdown-хуков (`System.exit(0)`). |