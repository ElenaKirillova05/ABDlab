-- ============================================================
-- Загрузка данных из staging_raw в нормализованную схему
-- ============================================================

-- 1. Загрузка поставщиков
INSERT INTO ref_vendor (vendor_name, vendor_contact_person, vendor_email_addr, vendor_phone_num, vendor_full_address, vendor_city_name, vendor_country_name)
SELECT DISTINCT
    vendor_name,
    vendor_contact,
    vendor_email,
    vendor_phone,
    vendor_address,
    vendor_city,
    vendor_country
FROM staging_raw
WHERE vendor_name IS NOT NULL AND vendor_name <> ''
ON CONFLICT (vendor_name, vendor_city_name) DO NOTHING;

-- 2. Загрузка покупателей
INSERT INTO ref_buyer (buyer_id, buyer_first_name, buyer_last_name, buyer_age_years, buyer_email_addr, buyer_country_name, buyer_postal_code, buyer_pet_type, buyer_pet_nickname, buyer_pet_breed)
SELECT DISTINCT ON (buyer_id::INT)
    buyer_id::INT,
    buyer_first_name,
    buyer_last_name,
    NULLIF(buyer_age, '')::INT,
    buyer_email,
    buyer_country,
    buyer_postal,
    buyer_pet_kind,
    buyer_pet_name,
    buyer_pet_breed
FROM staging_raw
WHERE buyer_id IS NOT NULL AND buyer_id <> ''
ORDER BY buyer_id::INT
ON CONFLICT (buyer_id) DO NOTHING;

-- 3. Загрузка продавцов
INSERT INTO ref_seller (seller_id, seller_first_name, seller_last_name, seller_email_addr, seller_country_name, seller_postal_code)
SELECT DISTINCT ON (seller_id::INT)
    seller_id::INT,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal
FROM staging_raw
WHERE seller_id IS NOT NULL AND seller_id <> ''
ORDER BY seller_id::INT
ON CONFLICT (seller_id) DO NOTHING;

-- 4. Загрузка товаров
INSERT INTO ref_item (item_id, item_name, item_category_name, item_unit_price, item_stock_qty, item_weight_kg, item_color, item_size_code, item_brand_name, item_material_type, item_long_desc, item_avg_rating, item_review_total, item_release_dt, item_expiration_dt, pet_category_name, vendor_id)
SELECT DISTINCT ON (src.item_id::INT)
    src.item_id::INT,
    src.item_name,
    src.item_category,
    NULLIF(src.item_price, '')::NUMERIC(10,2),
    NULLIF(src.item_quantity, '')::INT,
    NULLIF(src.item_weight, '')::NUMERIC(10,2),
    src.item_color,
    src.item_size,
    src.item_brand,
    src.item_material,
    src.item_description,
    NULLIF(src.item_rating, '')::NUMERIC(4,2),
    NULLIF(src.review_count, '')::INT,
    CASE WHEN src.release_date <> '' THEN TO_DATE(src.release_date, 'MM/DD/YYYY') ELSE NULL END,
    CASE WHEN src.expiry_date <> '' THEN TO_DATE(src.expiry_date, 'MM/DD/YYYY') ELSE NULL END,
    src.pet_type,
    vnd.vendor_id
FROM staging_raw src
LEFT JOIN ref_vendor vnd ON vnd.vendor_name = src.vendor_name AND COALESCE(vnd.vendor_city_name,'') = COALESCE(src.vendor_city,'')
WHERE src.item_id IS NOT NULL AND src.item_id <> ''
ORDER BY src.item_id::INT
ON CONFLICT (item_id) DO NOTHING;

-- 5. Загрузка магазинов
INSERT INTO ref_shop (shop_name, shop_location, shop_city_name, shop_state_code, shop_country_name, shop_phone_num, shop_email_addr)
SELECT DISTINCT
    shop_name,
    shop_address,
    shop_city,
    shop_state,
    shop_country,
    shop_phone,
    shop_email
FROM staging_raw
WHERE shop_name IS NOT NULL AND shop_name <> ''
ON CONFLICT (shop_name, shop_city_name) DO NOTHING;

-- 6. Загрузка календаря
INSERT INTO ref_calendar (full_date, day_num, month_num, year_num, quarter_num)
SELECT DISTINCT
    TO_DATE(transaction_date, 'MM/DD/YYYY') AS full_date,
    EXTRACT(DAY FROM TO_DATE(transaction_date, 'MM/DD/YYYY'))::INT AS day_num,
    EXTRACT(MONTH FROM TO_DATE(transaction_date, 'MM/DD/YYYY'))::INT AS month_num,
    EXTRACT(YEAR FROM TO_DATE(transaction_date, 'MM/DD/YYYY'))::INT AS year_num,
    EXTRACT(QUARTER FROM TO_DATE(transaction_date, 'MM/DD/YYYY'))::INT AS quarter_num
FROM staging_raw
WHERE transaction_date IS NOT NULL AND transaction_date <> ''
ON CONFLICT (full_date) DO NOTHING;

-- 7. Загрузка фактов продаж
INSERT INTO fact_transactions (buyer_ref_id, seller_ref_id, item_ref_id, shop_ref_id, date_ref_id, qty_sold, total_sale_amount)
SELECT
    src.buyer_id::INT,
    src.seller_id::INT,
    src.item_id::INT,
    sh.shop_id,
    cal.calendar_id,
    NULLIF(src.quantity_sold, '')::INT,
    NULLIF(src.total_amount, '')::NUMERIC(12,2)
FROM staging_raw src
LEFT JOIN ref_shop sh ON sh.shop_name = src.shop_name AND COALESCE(sh.shop_city_name,'') = COALESCE(src.shop_city,'')
LEFT JOIN ref_calendar cal ON cal.full_date = TO_DATE(src.transaction_date, 'MM/DD/YYYY')
WHERE src.buyer_id IS NOT NULL AND src.buyer_id <> '';
