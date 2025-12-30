-- =================================================================
-- PHẦN 1: TẦNG SILVER (Làm sạch, Làm phẳng, Lookup)
-- =================================================================

CREATE DATABASE IF NOT EXISTS silver;

-- 1. SILVER LOCATIONS (Gộp Province + Region) -----------------------
CREATE TABLE IF NOT EXISTS silver.locations
(
    province_id UInt32,
    province_name String,
    region_id UInt32,
    region_name String,
    latitude Nullable(Float32),
    longitude Nullable(Float32),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY province_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_bronze_to_silver_locations TO silver.locations AS
SELECT
    p.id AS province_id,
    p.province_name,
    r.id AS region_id,
    r.region_name,
    p.latitude,
    p.longitude,
    now() AS updated_at
FROM bronze.provinces AS p
INNER JOIN bronze.regions AS r ON p.region_id = r.id;

-- 2. SILVER CAMPAIGNS (Làm sạch) ----------------------------------
CREATE TABLE IF NOT EXISTS silver.campaigns
(
    campaign_id UInt32,
    campaign_title String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY campaign_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_bronze_to_silver_campaigns TO silver.campaigns AS
SELECT
    id AS campaign_id,
    campaign_title,
    now() AS updated_at
FROM bronze.adscampaigns;

-- 3. SILVER PRODUCTS (Làm phẳng Category, Brand) ------------------
CREATE TABLE IF NOT EXISTS silver.products
(
    product_id UInt32,
    product_name String,
    brand_id UInt32,
    brand_name String,
    category_id UInt32,
    category_name String,
    subcategory_id UInt32,
    subcategory_name String,
    unit_cost Decimal(18,2),
    current_price Decimal(18,2),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY product_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_bronze_to_silver_products TO silver.products AS
SELECT
    p.id AS product_id,
    p.product_name,
    ifNull(p.brand_id, 0) AS brand_id,
    ifNull(b.brand_name, 'No Brand') AS brand_name,
    
    -- Logic Category Cha/Con (Self-join)
    if(parent.id > 0, parent.id, sub.id) AS category_id,
    if(parent.id > 0, parent.category_name, sub.category_name) AS category_name,
    sub.id AS subcategory_id,
    sub.category_name AS subcategory_name,
    
    ifNull(p.unit_cost, 0) AS unit_cost,
    ifNull(p.product_price, 0) AS current_price,
    now() AS updated_at
FROM bronze.products AS p
LEFT JOIN bronze.brands AS b ON p.brand_id = b.id
LEFT JOIN bronze.categories AS sub ON p.category_id = sub.id
LEFT JOIN bronze.categories AS parent ON sub.category_id = parent.id;

-- 4. SILVER USERS ------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.users
(
    user_id UInt32,
    username String,
    registration_date Date,
    created_at DateTime,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY user_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_bronze_to_silver_users TO silver.users AS
SELECT
    id AS user_id,
    username,
    toDate(created_at) AS registration_date,
    CAST(created_at AS DateTime) AS created_at,
    now() AS updated_at
FROM bronze.users;

-- 5. SILVER ORDERS (Lookup Address -> City, Discount -> Campaign) --
CREATE TABLE IF NOT EXISTS silver.orders
(
    order_id UInt32,
    user_id UInt32,
    
    province_id UInt32,       -- Đã lookup
    campaign_key UInt32,  -- Đã lookup
    
    order_status_id UInt32,
    payment_method_id UInt32,
    shipping_method_id UInt32,
    
    total_amount Decimal(18,2),
    order_amount Decimal(18,2), -- Subtotal
    discount_amount Decimal(18,2),
    
    created_at DateTime,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_bronze_to_silver_orders TO silver.orders AS
SELECT
    o.id AS order_id,
    o.user_id,
    
    ifNull(a.province_id, 0) AS province_id,
    ifNull(d.adscampaign_id, 0) AS campaign_key,
    
    ifNull(o.order_status_id, 0) AS order_status_id,
    ifNull(o.payment_method_id, 0) AS payment_method_id,
    ifNull(o.shipping_method_id, 0) AS shipping_method_id,
    
    ifNull(o.total_amount, 0) AS total_amount,
    ifNull(o.order_amount, 0) AS order_amount,
    ifNull(o.discount_amount, 0) AS discount_amount,
    
    CAST(o.created_at AS DateTime) AS created_at,
    now() AS updated_at
FROM bronze.orders AS o
LEFT JOIN bronze.addresses AS a ON o.address_id = a.id
LEFT JOIN bronze.discounts AS d ON o.discount_id = d.id;

-- 6. SILVER ORDER ITEMS (Tính GMV) --------------------------------
CREATE TABLE IF NOT EXISTS silver.order_items
(
    order_id UInt32,
    product_id UInt32,
    quantity Int32,
    unit_price Decimal(18,2),
    gmv Decimal(18,2),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (order_id, product_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_bronze_to_silver_items TO silver.order_items AS
SELECT
    order_id,
    product_id,
    quantity,
    ifNull(product_price, 0) AS unit_price,
    (ifNull(quantity, 0) * ifNull(product_price, 0)) AS gmv,
    now() AS updated_at
FROM bronze.orderdetails;

--7. SILVER ORDER STATUS
CREATE TABLE IF NOT EXISTS silver.order_status
(
    id UInt32,
    name String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- 7.2. Đổ dữ liệu lịch sử (Backfill)
INSERT INTO silver.order_status (id, name, updated_at)
SELECT 
    id, 
    order_status_name AS name, 
    now()
FROM bronze.orderstatus;

-- 7.3. Tạo MV hứng dữ liệu mới (Real-time)
CREATE MATERIALIZED VIEW silver.mv_order_status TO silver.order_status AS
SELECT 
    id, 
    order_status_name AS name, 
    now() AS updated_at
FROM bronze.orderstatus;

-- =================================================================
-- PHẦN 2: TẦNG GOLD (Star Schema & Reporting)
-- =================================================================

CREATE DATABASE IF NOT EXISTS gold;

-- 1. DIMENSIONS (Tự động cập nhật từ Silver) ----------------------

-- Dim Locations
CREATE TABLE IF NOT EXISTS gold.dim_locations
(
    location_key UInt32,
    province_name String,
    region_name String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY location_key;

-- Init dòng Unknown
INSERT INTO gold.dim_locations VALUES (0, 'Unknown', 'Unknown', now());

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_silver_to_dim_locations TO gold.dim_locations AS
SELECT province_id AS location_key, province_name, region_name, updated_at FROM silver.locations;

-- Dim Products
CREATE TABLE IF NOT EXISTS gold.dim_products
(
    product_key UInt32,
    product_name String,
    category_name String,
    subcategory_name String,
    brand_name String,
    unit_cost Decimal(18,2),
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY product_key;

INSERT INTO gold.dim_products VALUES (0, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, now());

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_silver_to_dim_products TO gold.dim_products AS
SELECT product_id AS product_key, product_name, category_name, subcategory_name, brand_name, unit_cost, updated_at FROM silver.products;

-- Dim Campaigns
CREATE TABLE IF NOT EXISTS gold.dim_campaigns
(
    campaign_key UInt32,
    campaign_title String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY campaign_key;

INSERT INTO gold.dim_campaigns VALUES (0, 'No Campaign', now());

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_silver_to_dim_campaigns TO gold.dim_campaigns AS
SELECT campaign_id AS campaign_key, campaign_title, updated_at FROM silver.campaigns;

-- Dim Date (Tạo 1 lần)
CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date_key UInt32, full_date Date, year UInt16, quarter UInt8, month UInt8, day UInt8
) ENGINE = MergeTree() ORDER BY date_key;

INSERT INTO gold.dim_date
SELECT
    toYYYYMMDD(toDate('2020-01-01') + number) AS date_key,
    toDate('2020-01-01') + number AS full_date,
    toYear(full_date), toQuarter(full_date), toMonth(full_date), toDayOfMonth(full_date)
FROM numbers(3650); -- 10 năm

-- Dim Order Status
-- 1. Tạo bảng Dimension ở Gold
DROP TABLE IF EXISTS gold.dim_order_status;
CREATE TABLE gold.dim_order_status
(
    id UInt32,
    name String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- 2. Đổ dữ liệu từ Silver -> Gold
-- (Lần đầu chạy lệnh INSERT này để nạp dữ liệu)
INSERT INTO gold.dim_order_status SELECT * FROM silver.order_status;

-- 3. Tạo MV để Gold tự động cập nhật theo Silver
CREATE MATERIALIZED VIEW gold.mv_dim_order_status TO gold.dim_order_status AS
SELECT * FROM silver.order_status;


-- 2. FACT TABLES (Real-time MVs) ----------------------------------

-- FACT_USER_REGISTRATION
CREATE TABLE IF NOT EXISTS gold.FACT_USER_REGISTRATION
(
    date_key UInt32,
    user_amount SimpleAggregateFunction(sum, UInt64)
) ENGINE = SummingMergeTree() ORDER BY date_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_fact_user TO gold.FACT_USER_REGISTRATION AS
SELECT toYYYYMMDD(registration_date) AS date_key, count() AS user_amount
FROM silver.users GROUP BY date_key;

-- FACT_ORDER_OVERVIEW
DROP VIEW IF EXISTS gold.mv_fact_overview;
DROP TABLE IF EXISTS gold.FACT_ORDER_OVERVIEW;

-- 2. Tạo View Logic (Thay thế cho bảng Fact cứng)
CREATE VIEW gold.FACT_ORDER_OVERVIEW AS
SELECT
    -- Fix lỗi Timezone: Chuyển sang giờ VN trước khi cắt ngày
    toYYYYMMDD(toTimeZone(o.created_at, 'Asia/Ho_Chi_Minh')) AS date_key,
    
    o.province_id AS location_key,
    o.campaign_key,
    o.order_status_id,
    
    -- Lookup tên (Xử lý Null/Unknown)
    coalesce(pm.payment_method_name, 'Unknown') AS payment_method,
    coalesce(sm.shipping_method_name, 'Unknown') AS shipping_method,

    -- TÍNH TOÁN REAL-TIME (Aggregate)
    -- Logic: Đếm số dòng đã được gộp (FINAL) -> Ra số đơn chuẩn
    count() AS order_count,
    
    -- Logic: Cộng tổng tiền từ các dòng đã gộp -> Ra doanh thu chuẩn
    CAST(sum(o.total_amount) AS Decimal(38,2)) AS total_gmv

FROM silver.orders AS o FINAL -- <--- QUAN TRỌNG: Loại bỏ trùng lặp lịch sử
LEFT JOIN bronze.paymentmethods AS pm ON o.payment_method_id = pm.id
LEFT JOIN bronze.shippingmethods AS sm ON o.shipping_method_id = sm.id

GROUP BY 
    date_key, 
    location_key, 
    campaign_key, 
    order_status_id, 
    payment_method, 
    shipping_method;

-- 3. FACT TABLES (Scheduled Job - KHÔNG CÓ MV Ở ĐÂY) ---------------

CREATE TABLE IF NOT EXISTS gold.FACT_SALES_PRODUCT
(
    date_key UInt32,
    location_key UInt32,
    campaign_key UInt32,
    product_key UInt32,
    quantity SimpleAggregateFunction(sum, Int64),
    gmv SimpleAggregateFunction(sum, Decimal(38,2)),
    total_cost SimpleAggregateFunction(sum, Decimal(38,2)),
    discount_val SimpleAggregateFunction(sum, Decimal(38,2)),
    net_revenue SimpleAggregateFunction(sum, Decimal(38,2)),
    order_count SimpleAggregateFunction(sum, UInt64)
) ENGINE = SummingMergeTree() ORDER BY (date_key, location_key, campaign_key, product_key);