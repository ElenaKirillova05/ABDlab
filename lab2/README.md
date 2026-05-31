# Анализ больших данных - Лабораторная работа №2

## ETL пайплайн с использованием Apache Spark

### Описание работы

Apache Spark — один из самых популярных фреймворков для обработки больших данных. В данной работе реализован ETL-пайплайн, который трансформирует данные из CSV-файлов в аналитическую модель "Звезда" в PostgreSQL, а затем формирует аналитические отчеты в ClickHouse.

---

## Цель работы

Реализовать ETL-пайплайн с помощью Spark, который:
1. Загружает данные из CSV-файлов в PostgreSQL
2. Трансформирует данные в модель "Звезда" (таблицы фактов и измерений)
3. Формирует аналитические отчеты в ClickHouse

---

## Стек технологий

| Компонент | Технология | Назначение |
|-----------|------------|------------|
| Хранилище данных | PostgreSQL 15 | Исходные данные и Star Schema |
| ETL движок | Apache Spark 3.5 | Трансформация данных |
| Аналитическая БД | ClickHouse 23.8 | Хранение отчетов |
| Контейнеризация | Docker Compose | Запуск инфраструктуры |
| JDBC драйверы | PostgreSQL, ClickHouse | Подключение Spark к БД |

---

## Структура репозитория

BigDataSpark/
├── исходные данные/ # 10 CSV файлов (по 1000 строк)
├── init/ # Скрипты инициализации БД
│ ├── 01_create_source_table.sql
│ ├── 02_load_data.sh
│ └── 03_create_star_schema.sql
├── spark_jobs/ # Spark приложения
│ ├── transform_to_star_schema.py
│ └── generate_reports.py
├── jars/ # JDBC драйверы
├── docker-compose.yml
├── INSTRUCTIONS.md
└── README.md

---

## Модель данных

### Таблицы измерений (6 шт)

| Таблица | Описание | Ключевые поля |
|---------|----------|---------------|
| `ref_vendor` | Поставщики | vendor_id, vendor_name, vendor_country |
| `ref_item` | Товары | item_id, item_name, item_category, item_price |
| `ref_buyer` | Покупатели | buyer_id, buyer_first_name, buyer_country |
| `ref_agent` | Продавцы | agent_id, agent_first_name, agent_country |
| `ref_outlet` | Магазины | outlet_id, outlet_name, outlet_city, outlet_country |
| `ref_calendar` | Даты | calendar_id, full_date, year_num, month_num |

### Таблица фактов (1 шт)

| Таблица | Описание | Ключевые поля |
|---------|----------|---------------|
| `fact_transactions` | Продажи | buyer_id, agent_id, item_id, outlet_id, calendar_id, quantity_sold, total_amount |

---

## Отчеты (витрины данных)

### Витрина 1: Продажи по товарам
- Топ-10 самых продаваемых товаров
- Выручка по категориям товаров
- Средний рейтинг и количество отзывов

### Витрина 2: Продажи по клиентам
- Топ-10 клиентов по сумме покупок
- Распределение клиентов по странам
- Средний чек для каждого клиента

### Витрина 3: Продажи по времени
- Месячные и годовые тренды продаж
- Сравнение выручки по периодам
- Средний размер заказа по месяцам

### Витрина 4: Продажи по магазинам
- Топ-5 магазинов по выручке
- Распределение продаж по городам и странам
- Средний чек для каждого магазина

### Витрина 5: Продажи по поставщикам
- Топ-5 поставщиков по выручке
- Средняя цена товаров от каждого поставщика
- Распределение продаж по странам поставщиков

### Витрина 6: Качество продукции
- Товары с наивысшим и наименьшим рейтингом
- Корреляция рейтинга с объемом продаж
- Товары с наибольшим количеством отзывов

---

## Алгоритм выполнения работы

1. Клонировать репозиторий
2. Установить DBeaver для работы с SQL
3. Запустить инфраструктуру через Docker Compose
4. Дождаться загрузки всех CSV файлов в PostgreSQL
5. Проанализировать исходные данные с помощью SQL-запросов
6. Выявить сущности фактов и измерений
7. Запустить ETL процесс: загрузка данных в Star Schema
8. Запустить формирование отчетов в ClickHouse
9. Проверить отчеты через DBeaver
10. Отправить работу на проверку

---

## Результаты работы

- 10 CSV файлов (10000 строк) импортированы в PostgreSQL
- Данные трансформированы в Star Schema (6 измерений + 1 факт)
- Сформировано 6 аналитических отчетов в ClickHouse
- Все отчеты содержат агрегированные данные для анализа

---

## Быстрый старт

```bash
# Запуск инфраструктуры
docker compose up -d

# Запуск ETL трансформации
docker exec spark_master spark-submit \
  --master spark://spark_master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /opt/spark/jobs/transform_to_star_schema.py

# Запуск формирования отчетов
docker exec spark_master spark-submit \
  --master spark://spark_master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/clickhouse-jdbc-0.6.1-all.jar \
  /opt/spark/jobs/generate_reports.py


  -- Топ-10 товаров
SELECT * FROM top10_items_report;

-- Топ-10 покупателей
SELECT * FROM top10_buyers_report;

-- Месячные продажи
SELECT * FROM monthly_sales_analysis ORDER BY year_num, month_num;

-- Топ-5 магазинов
SELECT * FROM top5_outlets_report;

-- Топ-5 поставщиков
SELECT * FROM top5_vendors_report;

-- Анализ качества товаров
SELECT * FROM item_quality_analysis ORDER BY avg_score DESC LIMIT 10;
