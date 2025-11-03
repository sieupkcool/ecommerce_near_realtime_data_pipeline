-- Transactions Table and Materialized View ------------------------------
USE default;
CREATE TABLE IF NOT EXISTS transactions
(
    order_id UInt32,
    transaction_type LowCardinality(String),
    amount Nullable(Decimal(15, 2)),
    status LowCardinality(String),
    created_at Datetime64(3),
    ttl DateTime DEFAULT now()
)
-- Dùng ReplacingMergeTree để xử lý UPDATEs / DELETEs
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (order_id, created_at)
TTL ttl + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS _kafka_transactions
(
    -- Định nghĩa các cột Nullable, không dùng 'message'
    order_id Nullable(UInt32),
    transaction_type Nullable(String),
    amount Nullable(String), -- Debezium gửi decimal là String
    status Nullable(String),
    created_at Nullable(Int64) -- Debezium gửi timestamp là Int64 (microseconds)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.transactions',
    kafka_group_name = 'clickhouse_transaction_consumer',
    kafka_format = 'JSONEachRow', -- Tối ưu hiệu năng
    kafka_num_consumers = 2, -- Tăng consumer
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions TO transactions
(
    order_id UInt32,
    transaction_type String,
    amount Nullable(Decimal(15, 2)),
    status String,
    created_at Nullable(Datetime64(3)),
    ttl Nullable(DateTime)
) AS SELECT
    order_id,
    
    trim(lower(COALESCE(transaction_type, 'không rõ'))) AS transaction_type,
    toDecimal64OrNull(amount, 2) AS amount,
    trim(lower(COALESCE(status, 'không rõ'))) AS status,
    
    -- Debezium gửi Int64 (microseconds), phải chia 1000
    toDateTime64OrNull(created_at / 1000, 3) AS created_at,
    now() as ttl
FROM _kafka_transactions
WHERE order_id IS NOT NULL; -- Lọc message rác

-- Orders Table and Materialized View ---------------------------------
CREATE TABLE IF NOT EXISTS orders
(
    id UInt32,
    user_id UInt32,
    staff_id UInt32,
    address_id UInt32,
    order_amount Nullable(Decimal(15,2)),
    discount_amount Nullable(Decimal(15,2)),
    tax_amount Nullable(Decimal(15,2)),
    total_amount Nullable(Decimal(15,2)),
    discount_id Nullable(UInt32),
    payment_method_id Nullable(UInt32),
    payment_status_id Nullable(UInt32),
    order_status_id Nullable(UInt32),
    shipping_method_id Nullable(UInt32),
    shipping_status_id Nullable(UInt32),
    created_at Nullable(Datetime64(3)),
    ttl DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(id) -- Dùng ID làm khóa
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (id)
TTL ttl + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS _kafka_orders
(
    id Nullable(UInt32),
    user_id Nullable(UInt32),
    staff_id Nullable(UInt32),
    address_id Nullable(UInt32),
    order_amount Nullable(String),
    discount_amount Nullable(String),
    tax_amount Nullable(String),
    total_amount Nullable(String),
    discount_id Nullable(UInt32),
    payment_method_id Nullable(UInt32),
    payment_status_id Nullable(UInt32),
    order_status_id Nullable(UInt32),
    shipping_method_id Nullable(UInt32),
    shipping_status_id Nullable(UInt32),
    created_at Nullable(Int64)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.orders',
    kafka_group_name = 'clickhouse_order_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4, -- THAY ĐỔI: 1000 đơn/phút, cần nhiều consumer
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders TO orders
(
    id UInt32,
    user_id UInt32,
    staff_id UInt32,
    address_id UInt32,
    order_amount Nullable(Decimal(15,2)),
    discount_amount Nullable(Decimal(15,2)),
    tax_amount Nullable(Decimal(15,2)),
    total_amount Nullable(Decimal(15,2)),
    discount_id Nullable(UInt32),
    payment_method_id Nullable(UInt32),
    payment_status_id Nullable(UInt32),
    order_status_id Nullable(UInt32),
    shipping_method_id Nullable(UInt32),
    shipping_status_id Nullable(UInt32),
    created_at Nullable(Datetime64(3))
) AS SELECT
    id,
    user_id,
    staff_id,
    address_id,
    toDecimal64OrNull(order_amount, 2),
    toDecimal64OrNull(discount_amount, 2),
    toDecimal64OrNull(tax_amount, 2),
    toDecimal64OrNull(total_amount, 2),
    discount_id,
    payment_method_id,
    payment_status_id,
    COALESCE(order_status_id, 0) AS order_status_id, -- Gán 0 nếu status là null
    shipping_method_id,
    shipping_status_id,
    toDateTime64OrNull(created_at / 1000, 3) AS created_at
FROM _kafka_orders
WHERE id IS NOT NULL;

-- Products table ---------------------------------------------

CREATE TABLE IF NOT EXISTS products
(
	id UInt32,
	product_name String,
	category_id Nullable(UInt32),
	brand_id Nullable(UInt32),
	product_price Nullable(Float32)
)
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_products
(
    id Nullable(UInt32),
    product_name Nullable(String),
    category_id Nullable(UInt32),
    brand_id Nullable(UInt32),
    product_price Nullable(Float32) -- Giữ Float32 hoặc đổi sang String nếu Debezium gửi là String
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.products',
    kafka_group_name = 'clickhouse_products_consumer',
    kafka_format = 'JSONEachRow', 
    kafka_num_consumers = 2,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_products TO products
(
    id UInt32,
    product_name String,
    category_id Nullable(UInt32),
    brand_id Nullable(UInt32),
    product_price Nullable(Float32)
) AS SELECT
    id,
    trim(COALESCE(product_name, 'Không tên')) AS product_name,
    category_id,
    brand_id,
    product_price
FROM _kafka_products
WHERE id IS NOT NULL AND product_price >= 0;


-- Tags table ------------------------------------------

CREATE TABLE IF NOT EXISTS tags
(
    id UInt32,
    tag_name String
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_tags
(
    id Nullable(UInt32),
    tag_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.tags',
    kafka_group_name = 'clickhouse_tags_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tags TO tags
(
    id UInt32,
    tag_name String
) AS SELECT
    id,
    trim(lower(COALESCE(tag_name, 'không rõ'))) AS tag_name
FROM _kafka_tags
WHERE id IS NOT NULL;

-- Brands table ---------------------------------------------------

CREATE TABLE IF NOT EXISTS brands
(
    id UInt32,
    brand_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);


CREATE TABLE IF NOT EXISTS _kafka_brands
(
    id Nullable(UInt32),
    brand_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.brands',
    kafka_group_name = 'clickhouse_brands_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_brands TO brands
(
    id UInt32,
    brand_name String
) AS SELECT
    id,
    trim(COALESCE(brand_name, 'không rõ')) AS brand_name
FROM _kafka_brands
WHERE id IS NOT NULL;

-- Users table -------------------------------------------------------

CREATE TABLE IF NOT EXISTS users
(
    id UInt32,
    username String,
    created_at Nullable(Datetime64(3)),
    ttl DATETIME DEFAULT now()
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_users
(
    id Nullable(UInt32),
    username Nullable(String),
    created_at Nullable(Int64)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.users',
    kafka_group_name = 'clickhouse_users_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_users TO users
(
    id UInt32,
    username String,
    created_at Nullable(Datetime64(3)),
    ttl Nullable(DateTime)
) AS SELECT
    id,
    trim(COALESCE(username, 'người dùng ẩn')) AS username,
    toDateTime64OrNull(created_at / 1000, 3) AS created_at,
    now() as ttl
FROM _kafka_users
WHERE id IS NOT NULL;


-- roles table ------------------------------------------------

CREATE TABLE IF NOT EXISTS roles
(
    id UInt32,
    role_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_roles
(
    id Nullable(UInt32),
    role_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.roles',
    kafka_group_name = 'clickhouse_roles_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_roles TO roles
(
    id UInt32,
    role_name String
) AS SELECT
    id,
    trim(lower(COALESCE(role_name, 'không rõ'))) AS role_name
FROM _kafka_roles
WHERE id IS NOT NULL;

-- role_user table ----------------------------------------------

CREATE TABLE IF NOT EXISTS role_user
(
    id UInt32,
    role_id UInt32,
    user_id UInt32
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_role_user
(
    id Nullable(UInt32),
    role_id Nullable(UInt32),
    user_id Nullable(UInt32)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.role_user',
    kafka_group_name = 'clickhouse_role_user_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_role_user TO role_user
(
    id UInt32,
    role_id UInt32,
    user_id UInt32
) AS SELECT
    id,
    role_id,
    user_id
FROM _kafka_role_user
WHERE id IS NOT NULL;

-- provinces table --------------------------------------------

CREATE TABLE IF NOT EXISTS provinces
(
    id UInt32,
    province_name LowCardinality(String)
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_provinces
(
    id Nullable(UInt32),
    province_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.provinces',
    kafka_group_name = 'clickhouse_provinces_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_provinces TO provinces
(
    id UInt32,
    province_name String
) AS SELECT
    id,
    trim(lower(COALESCE(province_name, 'không rõ'))) AS province_name
FROM _kafka_provinces
WHERE id IS NOT NULL;

-- Cities table -------------------------------------------------

CREATE TABLE IF NOT EXISTS cities
(
    id UInt32,
    city_name LowCardinality(String),
    province_id UInt32,
    latitude Nullable(Float32),
    longitude Nullable(Float32)
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_cities
(
    id Nullable(UInt32),
    city_name Nullable(String),
    province_id Nullable(UInt32),
    latitude Nullable(Float32),
    longitude Nullable(Float32)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.cities',
    kafka_group_name = 'clickhouse_cities_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cities TO cities
(
    id UInt32,
    city_name String,
    province_id UInt32,
    latitude Nullable(Float32),
    longitude Nullable(Float32)
) AS SELECT
    id,
    trim(lower(COALESCE(city_name, 'không rõ'))) AS city_name,
    province_id,
    latitude,
    longitude
FROM _kafka_cities
WHERE id IS NOT NULL;

-- Addresses table ---------------------------------------------

CREATE TABLE IF NOT EXISTS addresses
(
    id UInt32,
    title LowCardinality(String),
    user_id UInt32,
    province_id UInt32,
    city_id UInt32
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_addresses
(
    id Nullable(UInt32),
    title Nullable(String),
    user_id Nullable(UInt32),
    province_id Nullable(UInt32),
    city_id Nullable(UInt32)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.addresses',
    kafka_group_name = 'clickhouse_addresses_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_addresses TO addresses
(
    id UInt32,
    title String,
    user_id UInt32,
    province_id UInt32,
    city_id UInt32
) AS SELECT
    id,
    trim(lower(COALESCE(title, 'không rõ'))) AS title,
    user_id,
    province_id,
    city_id
FROM _kafka_addresses
WHERE id IS NOT NULL;

-- Categories Table ---------------------------------------------

CREATE TABLE IF NOT EXISTS categories
(
    id UInt32,
    category_name LowCardinality(String),
    category_id Nullable(UInt32)
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_categories
(
    id Nullable(UInt32),
    category_name Nullable(String),
    category_id Nullable(UInt32)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.categories',
    kafka_group_name = 'clickhouse_categories_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_categories TO categories
(
    id UInt32,
    category_name String,
    category_id Nullable(UInt32)
) AS SELECT
    id,
    trim(lower(COALESCE(category_name, 'chưa phân loại'))) AS category_name,
    category_id
FROM _kafka_categories
WHERE id IS NOT NULL;

-- Product_Tag Table -------------------------------------
CREATE TABLE IF NOT EXISTS product_tag
(
    product_id UInt32,
    tag_id UInt32
) 
ENGINE = ReplacingMergeTree -- Không cần key, tự động xóa theo Primary Key
ORDER BY (product_id, tag_id); -- Khóa chính là (product_id, tag_id)

CREATE TABLE IF NOT EXISTS _kafka_product_tag
(
    product_id Nullable(UInt32),
    tag_id Nullable(UInt32)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.product_tag',
    kafka_group_name = 'clickhouse_product_tag_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_tag TO product_tag
(
    product_id UInt32,
    tag_id UInt32
) AS SELECT
    product_id,
    tag_id
FROM _kafka_product_tag
WHERE product_id IS NOT NULL AND tag_id IS NOT NULL;

-- AdsCampaign Table ----------------------------------

CREATE TABLE IF NOT EXISTS adscampaigns
(
    id UInt32,
    campaign_title LowCardinality(String)
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_adscampaign
(
    id Nullable(UInt32),
    campaign_title Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.adscampaigns',
    kafka_group_name = 'clickhouse_product_adscampaigns_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_adscampaigns TO adscampaigns
(
    id UInt32,
    campaign_title String
) AS SELECT
    id,
    trim(COALESCE(campaign_title, 'không rõ')) AS campaign_title
FROM _kafka_adscampaign
WHERE id IS NOT NULL;

-- Discounts Table -------------------------------------------------

CREATE TABLE IF NOT EXISTS discounts
(
    id UInt32,
    adscampaign_id Nullable(UInt32),
    type LowCardinality(String),
    value String,
    code String,
    started_at Nullable(Datetime64(3)),
    expired_at Nullable(Datetime64(3))
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_discounts
(
    id Nullable(UInt32),
    adscampaign_id Nullable(UInt32),
    type Nullable(String),
    value Nullable(String),
    code Nullable(String),
    started_at Nullable(Int64),
    expired_at Nullable(Int64)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.discounts',
    kafka_group_name = 'clickhouse_discounts_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_discounts TO discounts
(
    id UInt32,
    adscampaign_id Nullable(UInt32),
    type String,
    value String,
    code String,
    started_at Nullable(Datetime64(3)),
    expired_at Nullable(Datetime64(3))
) AS SELECT
    id,
    adscampaign_id,
    trim(lower(COALESCE(type, 'không rõ'))) AS type,
    trim(COALESCE(value, '0')) AS value,
    trim(upper(COALESCE(code, 'NOCODE'))) AS code,
    toDateTime64OrNull(started_at / 1000, 3) AS started_at,
    toDateTime64OrNull(expired_at / 1000, 3) AS expired_at
FROM _kafka_discounts
WHERE id IS NOT NULL;

-- OrderStatus Table -------------------------------------------

CREATE TABLE IF NOT EXISTS orderstatus
(
	id UInt32,
	order_status_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_orderstatus
(
    id Nullable(UInt32),
    order_status_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.orderstatus',
    kafka_group_name = 'clickhouse_orderstatus_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orderstatus TO orderstatus
(
    id UInt32,
    order_status_name String
) AS SELECT
    id,
    trim(COALESCE(order_status_name, 'Không rõ')) AS order_status_name
FROM _kafka_orderstatus
WHERE id IS NOT NULL;

-- PaymentMethods Table -------------------------------------------

CREATE TABLE IF NOT EXISTS paymentmethods
(
	id UInt32,
	payment_method_name String
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_paymentmethods
(
    id Nullable(UInt32),
    payment_method_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.paymentmethods',
    kafka_group_name = 'clickhouse_paymentmethods_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_paymentmethods TO paymentmethods
(
    id UInt32,
    payment_method_name String
) AS SELECT
    id,
    trim(COALESCE(payment_method_name, 'Không rõ')) AS payment_method_name
FROM _kafka_paymentmethods
WHERE id IS NOT NULL;

-- Payment Status Table-------------------------------------

CREATE TABLE IF NOT EXISTS paymentstatus
(
	id UInt32,
	payment_status_name String
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_paymentstatus
(
    id Nullable(UInt32),
    payment_status_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.paymentstatus',
    kafka_group_name = 'clickhouse_paymentstatus_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_paymentstatus TO paymentstatus
(
    id UInt32,
    payment_status_name String
) AS SELECT
    id,
    trim(COALESCE(payment_status_name, 'Không rõ')) AS payment_status_name
FROM _kafka_paymentstatus
WHERE id IS NOT NULL;

-- Shipping Status Table-------------------------------------

CREATE TABLE IF NOT EXISTS shippingstatus
(
	id UInt32,
	shipping_status_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_shippingstatus
(
    id Nullable(UInt32),
    shipping_status_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.shippingstatus',
    kafka_group_name = 'clickhouse_shippingstatus_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_shippingstatus TO shippingstatus
(
    id UInt32,
    shipping_status_name String
) AS SELECT
    id,
    trim(COALESCE(shipping_status_name, 'Không rõ')) AS shipping_status_name
FROM _kafka_shippingstatus
WHERE id IS NOT NULL;

-- Shipping Methods Table-------------------------------------

CREATE TABLE IF NOT EXISTS shippingmethods
(
	id UInt32,
	shipping_method_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);
CREATE TABLE IF NOT EXISTS _kafka_shippingmethods
(
    id Nullable(UInt32),
    shipping_method_name Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.shippingmethods',
    kafka_group_name = 'clickhouse_shippingmethods_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_shippingmethods TO shippingmethods
(
    id UInt32,
    shipping_method_name String
) AS SELECT
    id,
    trim(COALESCE(shipping_method_name, 'Không rõ')) AS shipping_method_name
FROM _kafka_shippingmethods
WHERE id IS NOT NULL;

-- OrderDetails Table-------------------------------------

CREATE TABLE IF NOT EXISTS orderdetails
(
    id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity Nullable(Int32),
    product_price Nullable(Decimal(15, 2)),
    product_tax Nullable(Decimal(15, 2)),
    subtotal_amount Nullable(Decimal(15, 2)),
    unit_cost Nullable(Decimal(15, 2)), 
    created_at Datetime64(3) DEFAULT now()
) 
ENGINE = ReplacingMergeTree(id) 
PARTITION BY (toYYYYMM(created_at))
ORDER BY (id);

CREATE TABLE IF NOT EXISTS _kafka_orderdetails
(
    id Nullable(UInt32),
    order_id Nullable(UInt32),
    product_id Nullable(UInt32),
    quantity Nullable(Int32),
    product_price Nullable(String),
    product_tax Nullable(String),
    subtotal_amount Nullable(String),
    unit_cost Nullable(String) -- THÊM CỘT unit_cost CHO DASHBOARD
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.orderdetails',
    kafka_group_name = 'clickhouse_orderdetails_consumer',
    kafka_format = 'JSONEachRow', 
    kafka_num_consumers = 4, -- THAY ĐỔI: Tăng consumer
    kafka_skip_broken_messages = 10;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orderdetails TO orderdetails
(
    id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity Nullable(Int32),
    product_price Nullable(Decimal(15, 2)),
    product_tax Nullable(Decimal(15, 2)),
    subtotal_amount Nullable(Decimal(15, 2)),
    unit_cost Nullable(Decimal(15, 2)),
    created_at Nullable(Datetime64(3))
) AS SELECT
    id,
    order_id,
    product_id,
    quantity,
    toDecimal64OrNull(product_price, 2),
    toDecimal64OrNull(product_tax, 2),
    toDecimal64OrNull(subtotal_amount, 2),
    toDecimal64OrNull(unit_cost, 2), 
    now() AS created_at 
FROM _kafka_orderdetails
-- Lọc dữ liệu rác
WHERE
    id IS NOT NULL AND quantity > 0;