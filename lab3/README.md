# Лабораторная работа №3: Потоковая обработка данных с Apache Flink

## Описание

Реализована потоковая обработка данных с использованием Apache Flink. Данные читаются из Kafka топика, трансформируются в схему "звезда" и сохраняются в PostgreSQL.

## Технологии

- Apache Flink 1.17
- Apache Kafka 7.5.0
- PostgreSQL 15
- Python 3.11
- Docker / Docker Compose

## Архитектура
CSV файлы → Kafka Producer → Kafka topic → Flink Streaming ETL → PostgreSQL (Star Schema)

## Структура проекта

BigDataFlink/
├── flink_job/
│ └── streaming_etl.py # Flink streaming приложение
├── init/
│ └── 01_create_star_schema.sql # Схема БД (звезда)
├── producer/
│ ├── Dockerfile # Docker образ продюсера
│ ├── kafka_producer.py # Отправка CSV в Kafka
│ └── requirements.txt # Зависимости Python
├── исходные данные/ # 10 CSV файлов (MOCK_DATA_*.csv)
├── docker-compose.yml # Оркестрация сервисов
├── INSTRUCTIONS.md # Инструкция по запуску
└── README.md # Описание проекта


## Схема базы данных (Star Schema)

### Измерения

| Таблица | Описание |
|---------|----------|
| dim_supplier | Поставщики |
| dim_product | Товары |
| dim_customer | Клиенты |
| dim_seller | Продавцы |
| dim_store | Магазины |
| dim_date | Даты |

### Факт-таблица

| Таблица | Описание |
|---------|----------|
| fact_sales | Продажи |

## Быстрый запуск

```bash
# 1. Запуск инфраструктуры
docker-compose up -d

# 2. Запуск продюсера (отправка данных в Kafka)
docker-compose up producer

# 3. Запуск Flink задачи (через Web UI)
# Открыть http://localhost:8081 → Submit New Job → streaming_etl.py
```
