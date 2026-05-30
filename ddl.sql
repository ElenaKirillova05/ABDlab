-- ============================================================
-- DDL: Создание схемы "Снежинка"
-- ============================================================

-- 1. Поставщики
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id      SERIAL PRIMARY KEY,
    supplier_name    VARCHAR(255) NOT NULL,
    supplier_contact VARCHAR(255),
    supplier_email   VARCHAR(255),
    supplier_phone   VARCHAR(50),
    supplier_address VARCHAR(500),
    supplier_city    VARCHAR(100),
    supplier_country VARCHAR(100),
    UNIQUE (supplier_name, supplier_city)
);

-- 2. Товары
CREATE TABLE IF NOT EXISTS dim_product (
    product_id          INT PRIMARY KEY,
    product_name        VARCHAR(255),
    product_category    VARCHAR(100),
    product_price       NUMERIC(10,2),
    product_quantity    INT,
    product_weight      NUMERIC(10,2),
    product_color       VARCHAR(100),
    product_size        VARCHAR(50),
    product_brand       VARCHAR(100),
    product_material    VARCHAR(100),
    product_description TEXT,
    product_rating      NUMERIC(4,2),
    product_reviews     INT,
    product_release_date DATE,
    product_expiry_date  DATE,
    pet_category        VARCHAR(100),
    supplier_id         INT REFERENCES dim_supplier(supplier_id)
);

-- 3. Покупатели
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id          INT PRIMARY KEY,
    customer_first_name  VARCHAR(100),
    customer_last_name   VARCHAR(100),
    customer_age         INT,
    customer_email       VARCHAR(255),
    customer_country     VARCHAR(100),
    customer_postal_code VARCHAR(20),
    customer_pet_type    VARCHAR(100),
    customer_pet_name    VARCHAR(100),
    customer_pet_breed   VARCHAR(100)
);

-- 4. Продавцы
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id          INT PRIMARY KEY,
    seller_first_name  VARCHAR(100),
    seller_last_name   VARCHAR(100),
    seller_email       VARCHAR(255),
    seller_country     VARCHAR(100),
    seller_postal_code VARCHAR(20)
);

-- 5. Магазины
CREATE TABLE IF NOT EXISTS dim_store (
    store_id       SERIAL PRIMARY KEY,
    store_name     VARCHAR(255),
    store_location VARCHAR(255),
    store_city     VARCHAR(100),
    store_state    VARCHAR(100),
    store_country  VARCHAR(100),
    store_phone    VARCHAR(50),
    store_email    VARCHAR(255),
    UNIQUE (store_name, store_city)
);

-- 6. Даты
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day       INT,
    month     INT,
    year      INT,
    quarter   INT
);

-- 7. Факт продаж
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id          SERIAL PRIMARY KEY,
    customer_id      INT REFERENCES dim_customer(customer_id),
    seller_id        INT REFERENCES dim_seller(seller_id),
    product_id       INT REFERENCES dim_product(product_id),
    store_id         INT REFERENCES dim_store(store_id),
    date_id          INT REFERENCES dim_date(date_id),
    sale_quantity    INT,
    sale_total_price NUMERIC(12,2)
);
