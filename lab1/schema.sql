-- ============================================================
-- Создание схемы "Снежинка"
-- ============================================================

-- Таблица: поставщики
CREATE TABLE IF NOT EXISTS ref_vendor (
    vendor_id          SERIAL PRIMARY KEY,
    vendor_name        VARCHAR(255) NOT NULL,
    vendor_contact_person VARCHAR(255),
    vendor_email_addr   VARCHAR(255),
    vendor_phone_num    VARCHAR(50),
    vendor_full_address VARCHAR(500),
    vendor_city_name    VARCHAR(100),
    vendor_country_name VARCHAR(100),
    UNIQUE (vendor_name, vendor_city_name)
);

-- Таблица: товары
CREATE TABLE IF NOT EXISTS ref_item (
    item_id             INT PRIMARY KEY,
    item_name           VARCHAR(255),
    item_category_name  VARCHAR(100),
    item_unit_price     NUMERIC(10,2),
    item_stock_qty      INT,
    item_weight_kg      NUMERIC(10,2),
    item_color          VARCHAR(100),
    item_size_code      VARCHAR(50),
    item_brand_name     VARCHAR(100),
    item_material_type  VARCHAR(100),
    item_long_desc      TEXT,
    item_avg_rating     NUMERIC(4,2),
    item_review_total   INT,
    item_release_dt     DATE,
    item_expiration_dt  DATE,
    pet_category_name   VARCHAR(100),
    vendor_id           INT REFERENCES ref_vendor(vendor_id)
);

-- Таблица: покупатели
CREATE TABLE IF NOT EXISTS ref_buyer (
    buyer_id            INT PRIMARY KEY,
    buyer_first_name    VARCHAR(100),
    buyer_last_name     VARCHAR(100),
    buyer_age_years     INT,
    buyer_email_addr    VARCHAR(255),
    buyer_country_name  VARCHAR(100),
    buyer_postal_code   VARCHAR(20),
    buyer_pet_type      VARCHAR(100),
    buyer_pet_nickname  VARCHAR(100),
    buyer_pet_breed     VARCHAR(100)
);

-- Таблица: продавцы
CREATE TABLE IF NOT EXISTS ref_seller (
    seller_id           INT PRIMARY KEY,
    seller_first_name   VARCHAR(100),
    seller_last_name    VARCHAR(100),
    seller_email_addr   VARCHAR(255),
    seller_country_name VARCHAR(100),
    seller_postal_code  VARCHAR(20)
);

-- Таблица: магазины
CREATE TABLE IF NOT EXISTS ref_shop (
    shop_id             SERIAL PRIMARY KEY,
    shop_name           VARCHAR(255),
    shop_location       VARCHAR(255),
    shop_city_name      VARCHAR(100),
    shop_state_code     VARCHAR(100),
    shop_country_name   VARCHAR(100),
    shop_phone_num      VARCHAR(50),
    shop_email_addr     VARCHAR(255),
    UNIQUE (shop_name, shop_city_name)
);

-- Таблица: календарь
CREATE TABLE IF NOT EXISTS ref_calendar (
    calendar_id         SERIAL PRIMARY KEY,
    full_date           DATE UNIQUE NOT NULL,
    day_num             INT,
    month_num           INT,
    year_num            INT,
    quarter_num         INT
);

-- Таблица фактов: продажи
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      SERIAL PRIMARY KEY,
    buyer_ref_id        INT REFERENCES ref_buyer(buyer_id),
    seller_ref_id       INT REFERENCES ref_seller(seller_id),
    item_ref_id         INT REFERENCES ref_item(item_id),
    shop_ref_id         INT REFERENCES ref_shop(shop_id),
    date_ref_id         INT REFERENCES ref_calendar(calendar_id),
    qty_sold            INT,
    total_sale_amount   NUMERIC(12,2)
);
