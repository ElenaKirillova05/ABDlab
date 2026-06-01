# Лабораторные работы по анализу больших данных

## Студент
Елена Кириллова

---

## Лабораторная работа №1 – Нормализация данных в модель "Снежинка"

### Цель
Трансформация исходных данных (10 CSV файлов по 1000 строк) в аналитическую модель "Снежинка" в PostgreSQL.

### Технологии
- PostgreSQL
- Docker
- SQL (DDL, DML)

### Структура
ab1/
├── init/ # Скрипты инициализации
├── исходные данные/ # 10 CSV файлов
├── schema.sql # DDL (создание таблиц)
├── data_load.sql # DML (заполнение данными)
├── docker-compose.yml
└── README.md

### Запуск
```bash
cd lab1
docker-compose up -d
# Выполнить скрипты через DBeaver в порядке:
# 1. schema.sql
# 2. data_load.sql
```
Результат
Таблица фактов: fact_transactions

Таблицы измерений: ref_buyer, ref_agent, ref_item, ref_outlet, ref_calendar, ref_vendor

---

## Лабораторная работа №2 – ETL с использованием Apache Spark
Цель
Реализовать ETL-пайплайн с помощью Spark: трансформация данных в модель "Звезда" в PostgreSQL и формирование отчетов в ClickHouse.

Технологии:
Apache Spark
PostgreSQL
ClickHouse
Docker

Структура
lab2/
├── init/                     # Скрипты инициализации
├── исходные данные/          # 10 CSV файлов
├── spark_jobs/               # Spark приложения
│   ├── transform_to_star_schema.py
│   └── generate_reports.py
├── jars/                     # JDBC драйверы
├── docker-compose.yml
└── INSTRUCTIONS.md

Запуск
```bash
cd lab2
docker-compose up -d
```

# ETL трансформация
docker exec spark_master spark-submit \
  --master spark://spark_master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /opt/spark/jobs/transform_to_star_schema.py

# Формирование отчетов
docker exec spark_master spark-submit \
  --master spark://spark_master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/clickhouse-jdbc-0.6.1-all.jar \
  /opt/spark/jobs/generate_reports.py

  Результат
6 аналитических отчетов в ClickHouse:

Отчет	                Описание
top10_items_report	Топ-10 товаров
top10_buyers_report	Топ-10 покупателей
monthly_sales_analysis	Продажи по месяцам
top5_outlets_report	Топ-5 магазинов
top5_vendors_report	Топ-5 поставщиков
item_quality_analysis	Качество товаров

---

## Лабораторная работа №3 – Streaming обработка с использованием Apache Flink
Цель
Реализовать потоковую обработку данных с помощью Flink: чтение из Kafka топика, трансформация в модель "Звезда" и запись в PostgreSQL.

Технологии:
Apache Flink 1.17
Apache Kafka 7.5.0
PostgreSQL 15
Python 3.11
Docker / Docker Compose

Архитектура
CSV файлы → Kafka Producer → Kafka topic → Flink Streaming ETL → PostgreSQL (Star Schema)

Структура
lab3/
├── flink_job/
│   └── streaming_etl.py             
├── init/
│   └── 01_create_star_schema.sql     
├── producer/
│   ├── Dockerfile                     
│   ├── kafka_producer.py              
│   └── requirements.txt               
├── исходные данные/                  
├── docker-compose.yml
├── INSTRUCTIONS.md
└── README.md

Запуск
```bash
cd lab3
docker-compose up -d
docker-compose up producer
# Открыть http://localhost:8081 → Submit New Job → streaming_etl.py
```
Схема "Звезда" в PostgreSQL:

Тип	Таблицы
Измерения	dim_supplier, dim_product, dim_customer, dim_seller, dim_store, dim_date
Факт	fact_sales

