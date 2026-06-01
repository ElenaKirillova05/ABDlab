"""
Лабораторная работа №3: Потоковая обработка данных
Тема: Streaming ETL из Kafka в PostgreSQL (схема звезда)
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import psycopg2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.datastream.functions import RichMapFunction

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'pet_shop_sales')
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = int(os.getenv('PG_PORT', 5432))
PG_DB = os.getenv('PG_DB', 'petshop')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'postgres')


def safe_int(value):
    """Безопасное преобразование в integer"""
    if value is None or str(value).strip() == '':
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def safe_float(value):
    """Безопасное преобразование в float"""
    if value is None or str(value).strip() == '':
        return None
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def safe_str(value, max_len=500):
    """Безопасное преобразование в строку"""
    if value is None:
        return None
    s = str(value).strip()
    return s[:max_len] if s else None


def parse_date(value):
    """Парсинг даты из формата mm/dd/yyyy"""
    if not value:
        return None
    try:
        date_str = str(value).strip()
        return datetime.strptime(date_str, '%m/%d/%Y').date()
    except (ValueError, TypeError):
        return None


class StreamingETLFunction(RichMapFunction):
    """RichMapFunction для потоковой обработки и загрузки в PostgreSQL"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.processed_count = 0
        self.error_count = 0
    
    def open(self, runtime_context):
        """Инициализация подключения к БД"""
        try:
            self.conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            logger.info("Подключение к PostgreSQL установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            raise
    
    def _upsert_supplier(self, row: Dict) -> Optional[int]:
        """Upsert поставщика"""
        supplier_name = safe_str(row.get('supplier_name'))
        if not supplier_name:
            return None
        
        # Проверяем существование
        self.cursor.execute("""
            SELECT supplier_id FROM dim_supplier 
            WHERE supplier_name = %s AND supplier_city = %s
        """, (supplier_name, safe_str(row.get('supplier_city'))))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Вставляем нового
        self.cursor.execute("""
            INSERT INTO dim_supplier (supplier_name, supplier_contact, supplier_email, 
                                      supplier_phone, supplier_address, supplier_city, supplier_country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (supplier_name, supplier_city) DO NOTHING
            RETURNING supplier_id
        """, (
            supplier_name,
            safe_str(row.get('supplier_contact')),
            safe_str(row.get('supplier_email')),
            safe_str(row.get('supplier_phone')),
            safe_str(row.get('supplier_address')),
            safe_str(row.get('supplier_city')),
            safe_str(row.get('supplier_country'))
        ))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Если не вставилось (конфликт), достаем существующего
        self.cursor.execute("""
            SELECT supplier_id FROM dim_supplier 
            WHERE supplier_name = %s AND supplier_city = %s
        """, (supplier_name, safe_str(row.get('supplier_city'))))
        return self.cursor.fetchone()[0]
    
    def _upsert_product(self, row: Dict, supplier_id: Optional[int]) -> Optional[int]:
        """Upsert товара"""
        product_id = safe_int(row.get('sale_product_id'))
        if not product_id:
            return None
        
        # Проверяем существование
        self.cursor.execute("SELECT product_id FROM dim_product WHERE product_id = %s", (product_id,))
        if self.cursor.fetchone():
            return product_id
        
        # Вставляем новый
        self.cursor.execute("""
            INSERT INTO dim_product (product_id, product_name, product_category, product_price,
                                     product_quantity, product_weight, product_color, product_size,
                                     product_brand, product_material, product_description,
                                     product_rating, product_reviews, product_release_date,
                                     product_expiry_date, pet_category, supplier_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        """, (
            product_id,
            safe_str(row.get('product_name')),
            safe_str(row.get('product_category')),
            safe_float(row.get('product_price')),
            safe_int(row.get('product_quantity')),
            safe_float(row.get('product_weight')),
            safe_str(row.get('product_color')),
            safe_str(row.get('product_size')),
            safe_str(row.get('product_brand')),
            safe_str(row.get('product_material')),
            safe_str(row.get('product_description')),
            safe_float(row.get('product_rating')),
            safe_int(row.get('product_reviews')),
            parse_date(row.get('product_release_date')),
            parse_date(row.get('product_expiry_date')),
            safe_str(row.get('pet_category')),
            supplier_id
        ))
        return product_id
    
    def _upsert_customer(self, row: Dict) -> Optional[int]:
        """Upsert клиента"""
        customer_id = safe_int(row.get('sale_customer_id'))
        if not customer_id:
            return None
        
        self.cursor.execute("""
            INSERT INTO dim_customer (customer_id, customer_first_name, customer_last_name,
                                      customer_age, customer_email, customer_country,
                                      customer_postal_code, customer_pet_type,
                                      customer_pet_name, customer_pet_breed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
        """, (
            customer_id,
            safe_str(row.get('customer_first_name')),
            safe_str(row.get('customer_last_name')),
            safe_int(row.get('customer_age')),
            safe_str(row.get('customer_email')),
            safe_str(row.get('customer_country')),
            safe_str(row.get('customer_postal_code')),
            safe_str(row.get('customer_pet_type')),
            safe_str(row.get('customer_pet_name')),
            safe_str(row.get('customer_pet_breed'))
        ))
        return customer_id
    
    def _upsert_seller(self, row: Dict) -> Optional[int]:
        """Upsert продавца"""
        seller_id = safe_int(row.get('sale_seller_id'))
        if not seller_id:
            return None
        
        self.cursor.execute("""
            INSERT INTO dim_seller (seller_id, seller_first_name, seller_last_name,
                                    seller_email, seller_country, seller_postal_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (seller_id) DO NOTHING
        """, (
            seller_id,
            safe_str(row.get('seller_first_name')),
            safe_str(row.get('seller_last_name')),
            safe_str(row.get('seller_email')),
            safe_str(row.get('seller_country')),
            safe_str(row.get('seller_postal_code'))
        ))
        return seller_id
    
    def _upsert_store(self, row: Dict) -> Optional[int]:
        """Upsert магазина"""
        store_name = safe_str(row.get('store_name'))
        if not store_name:
            return None
        
        # Проверяем существование
        self.cursor.execute("""
            SELECT store_id FROM dim_store 
            WHERE store_name = %s AND store_city = %s
        """, (store_name, safe_str(row.get('store_city'))))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Вставляем новый
        self.cursor.execute("""
            INSERT INTO dim_store (store_name, store_location, store_city, store_state,
                                   store_country, store_phone, store_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_name, store_city) DO NOTHING
            RETURNING store_id
        """, (
            store_name,
            safe_str(row.get('store_location')),
            safe_str(row.get('store_city')),
            safe_str(row.get('store_state')),
            safe_str(row.get('store_country')),
            safe_str(row.get('store_phone')),
            safe_str(row.get('store_email'))
        ))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Если не вставилось, достаем существующего
        self.cursor.execute("""
            SELECT store_id FROM dim_store 
            WHERE store_name = %s AND store_city = %s
        """, (store_name, safe_str(row.get('store_city'))))
        return self.cursor.fetchone()[0]
    
    def _upsert_date(self, date_str: str) -> Optional[int]:
        """Upsert даты"""
        date_val = parse_date(date_str)
        if not date_val:
            return None
        
        # Проверяем существование
        self.cursor.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (date_val,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Вставляем новую
        self.cursor.execute("""
            INSERT INTO dim_date (full_date, day, month, year, quarter)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (full_date) DO NOTHING
            RETURNING date_id
        """, (
            date_val,
            date_val.day,
            date_val.month,
            date_val.year,
            (date_val.month - 1) // 3 + 1
        ))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Если не вставилось, достаем существующую
        self.cursor.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (date_val,))
        return self.cursor.fetchone()[0]
    
    def _insert_fact(self, row: Dict, customer_id: Optional[int], seller_id: Optional[int],
                     product_id: Optional[int], store_id: Optional[int], date_id: Optional[int]) -> bool:
        """Вставка записи в факт-таблицу"""
        if not all([product_id, date_id]):
            return False
        
        self.cursor.execute("""
            INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, date_id,
                                    sale_quantity, sale_total_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            customer_id,
            seller_id,
            product_id,
            store_id,
            date_id,
            safe_int(row.get('sale_quantity')),
            safe_float(row.get('sale_total_price'))
        ))
        return True
    
    def map(self, value: str) -> str:
        """Основной метод обработки одного сообщения из Kafka"""
        try:
            # Парсим JSON
            row = json.loads(value)
        except json.JSONDecodeError as e:
            self.error_count += 1
            return f"ERROR: Invalid JSON - {str(e)[:50]}"
        
        if not row or not isinstance(row, dict):
            self.error_count += 1
            return "ERROR: Empty or invalid record"
        
        try:
            # Обрабатываем все измерения
            supplier_id = self._upsert_supplier(row)
            product_id = self._upsert_product(row, supplier_id)
            customer_id = self._upsert_customer(row)
            seller_id = self._upsert_seller(row)
            store_id = self._upsert_store(row)
            date_id = self._upsert_date(row.get('sale_date', ''))
            
            # Вставляем в факт-таблицу
            success = self._insert_fact(row, customer_id, seller_id, product_id, store_id, date_id)
            
            if success:
                self.conn.commit()
                self.processed_count += 1
                
                # Логируем прогресс каждые 100 записей
                if self.processed_count % 100 == 0:
                    logger.info(f"Обработано записей: {self.processed_count}, ошибок: {self.error_count}")
                
                return f"OK: id={row.get('id')} processed"
            else:
                self.conn.rollback()
                self.error_count += 1
                return f"ERROR: Missing required IDs (product={product_id}, date={date_id})"
                
        except Exception as e:
            self.conn.rollback()
            self.error_count += 1
            logger.error(f"Ошибка обработки записи {row.get('id')}: {e}")
            return f"ERROR: {str(e)[:100]}"
    
    def close(self):
        """Закрытие соединения с БД"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info(f"Итог обработки: успешно={self.processed_count}, ошибок={self.error_count}")


def main():
    logger.info("=" * 60)
    logger.info("ЗАПУСК STREAMING ETL ПРОЦЕССА")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    logger.info("=" * 60)
    
    # Создаем окружение Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Добавляем Kafka connector
    env.add_jars(
        "file:///opt/flink/lib/flink-connector-kafka-1.17.0.jar",
        "file:///opt/flink/lib/kafka-clients-3.4.0.jar",
    )
    
    # Настраиваем источник Kafka
    kafka_source = (KafkaSource.builder()
                    .set_bootstrap_servers(KAFKA_BOOTSTRAP)
                    .set_topics(KAFKA_TOPIC)
                    .set_group_id(f"flink_etl_{os.getpid()}")
                    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                    .set_value_only_deserializer(SimpleStringSchema())
                    .build())
    
    # Создаем поток
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka PetShop Source"
    )
    
    # Применяем ETL обработку
    result_stream = stream.map(StreamingETLFunction())
    result_stream.print()
    
    # Запускаем выполнение
    env.execute("PetShop_Streaming_ETL")


if __name__ == "__main__":
    main()
