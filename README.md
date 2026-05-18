# BigDataFlink

Лабораторная работа №3 по курсу "Анализ больших данных": streaming processing с помощью Apache Flink.

Проект реализует поток:

1. CSV файлы (`mock_data/*.csv`) -> JSON сообщения в Kafka.
2. Flink job читает Kafka topic в streaming-режиме.
3. Данные преобразуются в звездную схему.
4. Результат пишется в PostgreSQL.

## Состав проекта

- `mock_data/*.csv` - 10 файлов, по 1000 строк каждый.
- `app/csv_to_kafka_job.py` - отправка строк CSV в Kafka как JSON.
- `app/kafka_to_postgres_job.py` - Flink streaming job (Kafka -> PostgreSQL, star schema).
- `docker-compose.yml` - PostgreSQL + Kafka + Flink + автозапуск CSV producer.
- `Dockerfile` - образ Flink с Python-зависимостями и JDBC/Kafka коннекторами.

## Что разворачивается через Docker Compose

- `db` - PostgreSQL 15.
- `kafka` - Apache Kafka.
- `kafka-init` - создание топика `mock_data_topic`.
- `jobmanager`, `taskmanager` - кластер Flink.
- `csv-producer` - одноразовый запуск приложения, отправляющего данные из `mock_data/*.csv` в Kafka.

## Быстрый запуск

Из корня проекта:

```bash
docker compose up -d --build
```

Проверить, что producer отработал (должно быть сообщение про количество отправленных строк):

```bash
docker compose logs csv-producer
```

## Запуск Flink job

После запуска инфраструктуры отправьте job в Flink:

```bash
docker compose exec jobmanager flink run -py /opt/flink/app/kafka_to_postgres_job.py
```

Открыть Web UI Flink:

- [http://localhost:8081](http://localhost:8081)

## Проверка результата в PostgreSQL

Подключение:

- Host: `localhost`
- Port: `5432`
- DB: `BigData`
- User: `postgres`
- Password: `mysecretpassword`

Проверочные запросы:

```sql
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_products;
SELECT COUNT(*) FROM dim_stores;
SELECT COUNT(*) FROM dim_pets;
SELECT COUNT(*) FROM fact_sales;
```

После запуска Flink job ожидается до `10000` строк в `fact_sales` (по одной на каждое сообщение из Kafka).

Пример просмотра данных:

```sql
SELECT * FROM fact_sales LIMIT 20;

SELECT f.*, c.customer_first_name, p.product_category, s.store_city
FROM fact_sales f
JOIN dim_customers c ON c.customer_email = f.customer_email
JOIN dim_products p ON p.product_name = f.product_name
JOIN dim_stores s ON s.store_email = f.store_email
LIMIT 10;
```

## Остановка

```bash
docker compose down
```

Полная очистка тома Postgres:

```bash
docker compose down -v
```
