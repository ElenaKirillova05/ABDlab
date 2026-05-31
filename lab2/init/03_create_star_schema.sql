-- =====================================================
-- Создание схемы "Звезда" для аналитики продаж
-- =====================================================

-- Измерение: Поставщики
CREATE TABLE IF NOT EXISTS ref_vendor (
    vendor_id          SERIAL PRIMARY KEY,
    vendor_name        VARCHAR(255) NOT NULL,
    vendor_contact     VARCHAR(255),
    vendor_email       VARCHAR(255),
    vendor_phone       VARCHAR(50),
    vendor_address     VARCHAR(500),
    vendor_city        VARCHAR(100),
    vendor_country     VARCHAR(100),
    UNIQUE (vendor_name, vendor_city)
);

-- Измерение: Товары (связь с поставщиком)
CREATE TABLE IF NOT EXISTS ref_item (
    item_id             INT PRIMARY KEY,
    item_name           VARCHAR(255),
    item_category       VARCHAR(100),
    item_price          NUMERIC(10,2),
    item_quantity       INT,
    item_weight         NUMERIC(10,2),
    item_color          VARCHAR(100),
    item_size           VARCHAR(50),
    item_brand          VARCHAR(100),
    item_material       VARCHAR(100),
    item_description    TEXT,
    item_rating         NUMERIC(4,2),
    item_reviews        INT,
    item_release_date   DATE,
    item_expiry_date    DATE,
    pet_category        VARCHAR(100),
    vendor_id           INT REFERENCES ref_vendor(vendor_id)
);

-- Измерение: Покупатели
CREATE TABLE IF NOT EXISTS ref_buyer (
    buyer_id            INT PRIMARY KEY,
    buyer_first_name    VARCHAR(100),
    buyer_last_name     VARCHAR(100),
    buyer_age           INT,
    buyer_email         VARCHAR(255),
    buyer_country       VARCHAR(100),
    buyer_postal_code   VARCHAR(20),
    buyer_pet_type      VARCHAR(100),
    buyer_pet_name      VARCHAR(100),
    buyer_pet_breed     VARCHAR(100)
);

-- Измерение: Продавцы
CREATE TABLE IF NOT EXISTS ref_agent (
    agent_id            INT PRIMARY KEY,
    agent_first_name    VARCHAR(100),
    agent_last_name     VARCHAR(100),
    agent_email         VARCHAR(255),
    agent_country       VARCHAR(100),
    agent_postal_code   VARCHAR(20)
);

-- Измерение: Магазины
CREATE TABLE IF NOT EXISTS ref_outlet (
    outlet_id           SERIAL PRIMARY KEY,
    outlet_name         VARCHAR(255),
    outlet_location     VARCHAR(255),
    outlet_city         VARCHAR(100),
    outlet_state        VARCHAR(100),
    outlet_country      VARCHAR(100),
    outlet_phone        VARCHAR(50),
    outlet_email        VARCHAR(255),
    UNIQUE (outlet_name, outlet_city)
);

-- Измерение: Календарь
CREATE TABLE IF NOT EXISTS ref_calendar (
    calendar_id         SERIAL PRIMARY KEY,
    full_date           DATE UNIQUE NOT NULL,
    day_num             INT,
    month_num           INT,
    year_num            INT,
    quarter_num         INT
);

-- Таблица фактов: Продажи
CREATE TABLE IF NOT EXISTS fact_transactions (
    trans_id            SERIAL PRIMARY KEY,
    buyer_id            INT REFERENCES ref_buyer(buyer_id),
    agent_id            INT REFERENCES ref_agent(agent_id),
    item_id             INT REFERENCES ref_item(item_id),
    outlet_id           INT REFERENCES ref_outlet(outlet_id),
    calendar_id         INT REFERENCES ref_calendar(calendar_id),
    quantity_sold       INT,
    total_amount        NUMERIC(12,2)
);
