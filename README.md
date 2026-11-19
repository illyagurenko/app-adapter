# App Adapter Service

Высокопроизводительный интеграционный шлюз пакетной вычитки финансовых проводок из `GRU.GRU_VISTA_TAB` (Oracle), публикации батчей в Apache Kafka и асинхронной фиксации ответов с обновлением балансов и отбраковкой.

Реализован на **Java 21**, **Spring Boot 3.3.5**, **MyBatis 3** и **Liquibase** с поддержкой горизонтального масштабирования (2 параллельных инстанса с `FOR UPDATE SKIP LOCKED`).

---

## Архитектура и схема работы

Сервис обеспечивает непрерывный двунаправленный конвейер между Oracle и брокером Kafka:

```text
               [ База данных Oracle: GRU.GRU_VISTA_TAB ]
                                   │
                                   ▼
             ┌───────────────────────────────────────────┐
             │            GruProducerWorker              │
             │  (FOR UPDATE SKIP LOCKED, порции по 100)  │
             └───────────────────────────────────────────┘
                                   │
                                   ▼
   ┌───────────────────────────────────────────────────────────┐
   │  Таблица APP_ADAPTER_TRANS (STATUS: 'CONSUMER_WAIT')      │
   │  └──► Аудит: APP_ADAPTER_IO_MSGS (DIR: 'OUT', TIMESTAMP)  │
   └───────────────────────────────────────────────────────────┘
                                   │
                                   ▼
              [ Kafka Topic IN: gru.balance.request ]
                                   │
                                   ▼
                       [ Сервис app-changer ]
                        (Расчет остатков)
                                   │
                                   ▼
              [ Kafka Topic OUT: gru.balance.response ]
                                   │
                                   ▼
             ┌───────────────────────────────────────────┐
             │             AppKafkaConsumer              │
             │           (GruConsumerService)            │
             └───────────────────────────────────────────┘
                                   │
                                   ▼
   ┌───────────────────────────────────────────────────────────┐
   │  Таблица APP_ADAPTER_TRANS (STATUS: 'CONSUMER_IN_PROCESS')│
   │  └──► Аудит: APP_ADAPTER_IO_MSGS (DIR: 'IN', TIMESTAMP)   │
   └───────────────────────────────────────────────────────────┘
                                   │
                                   ▼
             ┌───────────────────────────────────────────┐
             │            GruValidateService             │
             │   (Бизнес-валидация и проверка пачки)     │
             └───────────────────────────────────────────┘
                       /                       \
                      ▼                         ▼
         ┌─────────────────────────┐ ┌─────────────────────────┐
         │     Успешные строки     │ │    Брак / Сбой пачки    │
         │  FOC_STATUS = 'SUCCESS' │ │  FOC_STATUS = 'ERROR'   │
         │  TIME_STAMP_UPDATE = now│ │  TIME_STAMP_UPDATE=null │
         │  Обновление OLDTBAL /   │ │  Запись в таблицу       │
         │  NEWTBAL                │ │  GRU.GRU_REJECT_TAB     │
         └─────────────────────────┘ └─────────────────────────┘
```

---

## Стек технологий

* **Язык:** Java 21 (Records, Pattern Matching, Virtual Threads ready)
* **Фреймворк:** Spring Boot 3.3.5 (Spring Web, Spring Kafka, Actuator)
* **База данных:** Oracle Database (XE / 19c) с поддержкой TNS Failover Descriptor
* **Доступ к данным:** MyBatis 3 (изолированные XML-мапперы со строгим контролем SQL)
* **Брокер сообщений:** Apache Kafka (`spring-kafka`)
* **Миграции БД:** Liquibase
* **Сборщик:** Gradle 9.0

---

## Порты и сетевая архитектура

Сервис разворачивается в Docker-сети `food_bridge` в двух параллельных экземплярах:

| Контейнер | Назначение | Порт хоста | Внутренний порт | PC_NUMBER |
| :--- | :--- | :--- | :--- | :--- |
| **`app_adapter_1`** | Инстанс 1 (Нода 161) | `8082` | `8082` | `161` |
| **`app_adapter_2`** | Инстанс 2 (Нода 162) | `8083` | `8082` | `162` |

---

## Георезервирование (High Availability)

Конфигурация высокой доступности настраивается через переменные окружения в файле `.env`:

1. **Oracle TNS Failover Descriptor:**
   ```text
   jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(TRANSPORT_CONNECT_TIMEOUT=3)(RETRY_COUNT=3)(ADDRESS_LIST=(FAILOVER=ON)(LOAD_BALANCE=OFF)(ADDRESS=(PROTOCOL=TCP)(HOST=${ORACLE_HOST_PRIMARY})(PORT=${ORACLE_PORT_PRIMARY}))(ADDRESS=(PROTOCOL=TCP)(HOST=${ORACLE_HOST_STANDBY})(PORT=${ORACLE_PORT_STANDBY})))(CONNECT_DATA=(SERVICE_NAME=${ORACLE_SERVICE_NAME})))
   ```
2. **Kafka Dual-Broker Failover:**
   `SPRING_KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BROKER_PRIMARY},${KAFKA_BROKER_STANDBY}`
3. **Changer REST Client:**
   `CHANGER_URL=http://app-changer-1:8080` (обращение по Docker DNS к сервису `app-changer-1`).

Пример содержимого файла `.env`:
```properties
ORACLE_HOST_PRIMARY=food_card_oracle
ORACLE_PORT_PRIMARY=1521
ORACLE_HOST_STANDBY=food_card_oracle
ORACLE_PORT_STANDBY=1521
ORACLE_SERVICE_NAME=XE

KAFKA_BROKER_PRIMARY=test_kafka:29092
KAFKA_BROKER_STANDBY=test_kafka:29092

CHANGER_URL=http://app-changer-1:8080
```

---

## База данных и Liquibase

### 1. Схема `ACC_APP_ADAPTER` (управляется адаптером):
* `APP_ADAPTER_CONFIG` — параметры топиков и флаги активности воркеров (`STATUS_IN`, `STATUS_OUT`).
* `APP_ADAPTER_TRANS` — аудит транзакций пачек (`CONSUMER_WAIT`, `CONSUMER_IN_PROCESS`, `SUCCESS`, `ERROR`).
* `APP_ADAPTER_IO_MSGS` — аудит сырых сообщений. Поле `INS_TS` имеет тип `TIMESTAMP(3)` для точного замера сетевой задержки доставки сообщений в миллисекундах.

### 2. Схема `GRU` (бизнес-схема):
* `GRU.GRU_VISTA_TAB`:
  * `FOC_STATUS`: `WAIT`, `IN_PROCESS`, `SUCCESS`, `ERROR`.
  * `FOC_TYPE` и `FOC_TS`: для `IMMEDIATE` поле `FOC_TS` строго `NULL`; для `INTIME` поле `FOC_TS` хранит плановое время исполнения (`FOC_TS <= SYSDATE`).
  * `TIME_STAMP_UPDATE`: заполняется текущим временем `SYSDATE` только при успешном завершении (`SUCCESS`).
  * `OPERATION`: строго банковский enum `Type` (`CR` — кредит, `DR` — дебет, `ZR` — зачисление).
* `GRU.GRU_REJECT_TAB` — реестр забракованных проводок.

---

## REST API сервиса

Базовый путь: `http://localhost:8082` (инстанс 1) или `http://localhost:8083` (инстанс 2).

### 1. Замер задержек доставки сообщений через Kafka:
* **`GET /api/admin/latency/from-changer`**  
  Вычисляет время (в миллисекундах), за которое `app-adapter` получил ответ от `app-changer` через Kafka. Опрашивает REST `app-changer` за временем отправки (`DIR=OUT`) и сопоставляет с временем записи в `APP_ADAPTER_IO_MSGS` (`DIR=IN`).
* **`GET /api/admin/latency/from-producer`**  
  Вычисляет задержку доставки запроса от `app-adapter` до `app-changer` через Kafka.

Пример ответа:
```json
{
  "direction": "PRODUCER_TO_CHANGER",
  "status": "OK",
  "requestId": "97330dcb-a8ec-482e-91bf-e697e85b2441",
  "sentAt": "2026-11-19T11:14:14.763Z",
  "receivedAt": "2026-11-19T08:14:14.763003Z",
  "latencyMs": 0
}
```

### 2. Административное управление:
* `GET /api/admin/status` — текущий статус активности продюсеров и консьюмеров.
* `POST /api/admin/workers/stop` — остановить воркеры Kafka.
* `POST /api/admin/restart` — перечитать конфигурацию из БД и перезапустить потоки.
* `POST /api/admin/shutdown` — корректная остановка JVM (Graceful Shutdown).

---

## Пошаговая инструкция по запуску с нуля (Clone & Run Guide)

### Шаг 1. Клонирование репозитория
```bash
git clone https://github.com/illyagurenko/app-adapter
cd app-adapter
```

### Шаг 2. Настройка переменных окружения (`.env`)
Создайте файл `.env` в корне проекта (или скопируйте из `.env.example`):
```bash
cp .env.example .env
```
Пример содержимого `.env`:
```properties
ORACLE_HOST_PRIMARY=food_card_oracle
ORACLE_PORT_PRIMARY=1521
ORACLE_HOST_STANDBY=food_card_oracle
ORACLE_PORT_STANDBY=1521
ORACLE_SERVICE_NAME=XE

KAFKA_BROKER_PRIMARY=test_kafka:29092
KAFKA_BROKER_STANDBY=test_kafka:29092

CHANGER_URL=http://app-changer-1:8080
```

### Шаг 3. Проверка Docker-сети
Убедитесь, что общая сеть создана:
```bash
docker network create food_bridge
```

### Шаг 4. Проверка инфраструктуры
Для работы адаптера должны быть предварительно запущены:
* Oracle XE (`food_card_oracle` на порту `1521`).
* Apache Kafka (`test_kafka` на порту `9092`).
* Пользователь `ACC_APP_ADAPTER` в Oracle (создается автоматически при старте контейнера `food_card_oracle` в сервисе `publisher-change-food-card`).

### Шаг 5. Сборка JAR-файла через Gradle
* **Для Windows (PowerShell / CMD):**
  ```powershell
  .\gradlew.bat clean build -x test
  ```
* **Для Linux / macOS:**
  ```bash
  ./gradlew clean build -x test
  ```

### Шаг 6. Сборка Docker-образа и запуск двух инстансов
```bash
docker compose build
docker compose up -d
```

### Шаг 7. Проверка работоспособности
1. **Проверка контейнеров:**
   ```bash
   docker compose ps
   ```
   Контейнеры `app_adapter_1` и `app_adapter_2` должны быть в статусе `Up`.

2. **Проверка логов:**
   Логи пишутся в директорию `./logs`:
   ```bash
   # Просмотр логов в реальном времени
   tail -f logs/app.log
   ```
   В логах должно появиться:
   ```text
   INFO r.i.i.config.AppInitializer - Workers initialized. running=true, producers=2, consumers=2
   ```

3. **Проверка статуса через REST:**
   ```bash
   curl.exe http://localhost:8082/api/admin/status
   curl.exe http://localhost:8083/api/admin/status
   ```
