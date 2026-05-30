-- ============================================================
-- DML: Заполнение схемы "Снежинка" из mock_data
-- ============================================================

-- 1. Заполнение dim_supplier
INSERT INTO dim_supplier (supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data
WHERE supplier_name IS NOT NULL AND supplier_name <> ''
ON CONFLICT (supplier_name, supplier_city) DO NOTHING;

-- 2. Заполнение dim_customer
INSERT INTO dim_customer (customer_id, customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed)
SELECT DISTINCT ON (sale_customer_id::INT)
    sale_customer_id::INT,
    customer_first_name,
    customer_last_name,
    NULLIF(customer_age, '')::INT,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data
WHERE sale_customer_id IS NOT NULL AND sale_customer_id <> ''
ORDER BY sale_customer_id::INT
ON CONFLICT (customer_id) DO NOTHING;

-- 3. Заполнение dim_seller
INSERT INTO dim_seller (seller_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code)
SELECT DISTINCT ON (sale_seller_id::INT)
    sale_seller_id::INT,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data
WHERE sale_seller_id IS NOT NULL AND sale_seller_id <> ''
ORDER BY sale_seller_id::INT
ON CONFLICT (seller_id) DO NOTHING;

-- 4. Заполнение dim_product
INSERT INTO dim_product (product_id, product_name, product_category, product_price, product_quantity, product_weight, product_color, product_size, product_brand, product_material, product_description, product_rating, product_reviews, product_release_date, product_expiry_date, pet_category, supplier_id)
SELECT DISTINCT ON (m.sale_product_id::INT)
    m.sale_product_id::INT,
    m.product_name,
    m.product_category,
    NULLIF(m.product_price, '')::NUMERIC(10,2),
    NULLIF(m.product_quantity, '')::INT,
    NULLIF(m.product_weight, '')::NUMERIC(10,2),
    m.product_color,
    m.product_size,
    m.product_brand,
    m.product_material,
    m.product_description,
    NULLIF(m.product_rating, '')::NUMERIC(4,2),
    NULLIF(m.product_reviews, '')::INT,
    CASE WHEN m.product_release_date <> '' THEN TO_DATE(m.product_release_date, 'MM/DD/YYYY') ELSE NULL END,
    CASE WHEN m.product_expiry_date <> '' THEN TO_DATE(m.product_expiry_date, 'MM/DD/YYYY') ELSE NULL END,
    m.pet_category,
    s.supplier_id
FROM mock_data m
LEFT JOIN dim_supplier s ON s.supplier_name = m.supplier_name AND COALESCE(s.supplier_city,'') = COALESCE(m.supplier_city,'')
WHERE m.sale_product_id IS NOT NULL AND m.sale_product_id <> ''
ORDER BY m.sale_product_id::INT
ON CONFLICT (product_id) DO NOTHING;

-- 5. Заполнение dim_store
INSERT INTO dim_store (store_name, store_location, store_city, store_state, store_country, store_phone, store_email)
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data
WHERE store_name IS NOT NULL AND store_name <> ''
ON CONFLICT (store_name, store_city) DO NOTHING;

-- 6. Заполнение dim_date
INSERT INTO dim_date (full_date, day, month, year, quarter)
SELECT DISTINCT
    TO_DATE(sale_date, 'MM/DD/YYYY') AS full_date,
    EXTRACT(DAY FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT AS day,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT AS month,
    EXTRACT(YEAR FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT AS year,
    EXTRACT(QUARTER FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT AS quarter
FROM mock_data
WHERE sale_date IS NOT NULL AND sale_date <> ''
ON CONFLICT (full_date) DO NOTHING;

-- 7. Заполнение fact_sales
INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, date_id, sale_quantity, sale_total_price)
SELECT
    m.sale_customer_id::INT,
    m.sale_seller_id::INT,
    m.sale_product_id::INT,
    st.store_id,
    d.date_id,
    NULLIF(m.sale_quantity, '')::INT,
    NULLIF(m.sale_total_price, '')::NUMERIC(12,2)
FROM mock_data m
LEFT JOIN dim_store st ON st.store_name = m.store_name AND COALESCE(st.store_city,'') = COALESCE(m.store_city,'')
LEFT JOIN dim_date  d  ON d.full_date = TO_DATE(m.sale_date, 'MM/DD/YYYY')
WHERE m.sale_customer_id IS NOT NULL AND m.sale_customer_id <> '';
