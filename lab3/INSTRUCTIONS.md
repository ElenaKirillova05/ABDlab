# Инструкция по запуску лабораторной работы №3

## Архитектура
CSV файлы → Kafka Producer → Kafka topic "pet_shop_sales" → Flink Streaming ETL → PostgreSQL (Star Schema)


## Шаги запуска

### 1. Запуск инфраструктуры

```bash
docker-compose up -d
docker-compose ps
docker-compose up producer

проверка

docker exec flink_kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic pet_shop_sales \
  --from-beginning \
  --max-messages 5
```

-- Количество записей в таблицах
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM dim_store;
SELECT COUNT(*) FROM dim_date;

-- Пример анализа: топ-5 товаров по выручке
SELECT 
    p.product_name,
    SUM(f.sale_quantity) as quantity,
    SUM(f.sale_total_price) as revenue
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 5;

-- Продажи по месяцам
SELECT 
    d.year,
    d.month,
    COUNT(*) as sales_count,
    SUM(f.sale_total_price) as total_revenue
FROM fact_sales f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

# Остановка всех сервисов
docker-compose down

# Полная очистка (удаление всех данных)
docker-compose down -v
