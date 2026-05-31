# Инструкция по запуску лабораторной работы №2 (Analytics Pipeline)

## Предварительные требования

- Установлены Docker и Docker Compose
- Папка `jars/` в корне проекта должна содержать:
  - `postgresql-42.7.3.jar` — JDBC драйвер для PostgreSQL
  - `clickhouse-jdbc-0.6.1-all.jar` — JDBC драйвер для ClickHouse

> **Примечание:** Файлы можно скачать с официальных сайтов или из репозитория преподавателя.

---

## Шаги запуска

### 1. Запуск инфраструктуры

```bash
docker compose up -d postgres_db clickhouse_db spark_master spark_worker jupyter_lab
