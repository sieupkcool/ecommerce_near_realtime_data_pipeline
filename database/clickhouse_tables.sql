-- Transactions Table and Materialized View ------------------------------

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.transactions
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

CREATE TABLE IF NOT EXISTS bronze.kafka_transactions
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_transactions TO bronze.transactions
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
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    now() as ttl
FROM bronze.kafka_transactions
WHERE order_id IS NOT NULL; -- Lọc message rác

-- Orders Table and Materialized View ---------------------------------
CREATE TABLE IF NOT EXISTS bronze.orders
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
TTL ttl + INTERVAL 7 DAY
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS bronze.kafka_orders
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
    shipped_at Nullable(Int64),
    created_at Nullable(Int64),
    updated_at Nullable(Int64)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.orders',
    kafka_group_name = 'clickhouse_order_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4, -- THAY ĐỔI: 1000 đơn/phút, cần nhiều consumer
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orders TO bronze.orders
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
    toDecimal64OrNull(order_amount, 2) AS order_amount,
    toDecimal64OrNull(discount_amount, 2) AS discount_amount,
    toDecimal64OrNull(tax_amount, 2) AS tax_amount,
    toDecimal64OrNull(total_amount, 2) AS total_amount,
    discount_id,
    payment_method_id,
    payment_status_id,
    COALESCE(order_status_id, 0) AS order_status_id, -- Gán 0 nếu status là null
    shipping_method_id,
    shipping_status_id,
    fromUnixTimestamp64Milli(created_at) AS created_at
FROM bronze.kafka_orders
WHERE id IS NOT NULL;

-- Products table ---------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.products
(
	id UInt32,
	product_name String,
	category_id Nullable(UInt32),
	brand_id Nullable(UInt32),
	product_price Nullable(Float32),
    unit_cost Nullable(Decimal(15, 2))
)
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_products
(
    id Nullable(UInt32),
    product_name Nullable(String),
    category_id Nullable(UInt32),
    brand_id Nullable(UInt32),
    product_price Nullable(Float32), -- Giữ Float32 hoặc đổi sang String nếu Debezium gửi là String
    unit_cost Nullable(String)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.products',
    kafka_group_name = 'clickhouse_products_consumer',
    kafka_format = 'JSONEachRow', 
    kafka_num_consumers = 2,
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_products TO bronze.products
(
    id UInt32,
    product_name String,
    category_id Nullable(UInt32),
    brand_id Nullable(UInt32),
    product_price Nullable(Float32),
    unit_cost Nullable(Decimal(15, 2))
) AS SELECT
    id,
    trim(COALESCE(product_name, 'Không tên')) AS product_name,
    category_id,
    brand_id,
    product_price,
    toDecimal64OrNull(unit_cost, 2) AS unit_cost
FROM bronze.kafka_products
WHERE id IS NOT NULL AND product_price >= 0;


-- Tags table ------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.tags
(
    id UInt32,
    tag_name String
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_tags
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_tags TO bronze.tags
(
    id UInt32,
    tag_name String
) AS SELECT
    id,
    trim(lower(COALESCE(tag_name, 'không rõ'))) AS tag_name
FROM bronze.kafka_tags
WHERE id IS NOT NULL;

-- Brands table ---------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.brands
(
    id UInt32,
    brand_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);


CREATE TABLE IF NOT EXISTS bronze.kafka_brands
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


CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_brands TO bronze.brands
(
    id UInt32,
    brand_name String
) AS SELECT
    id,
    trim(COALESCE(brand_name, 'không rõ')) AS brand_name
FROM bronze.kafka_brands
WHERE id IS NOT NULL;

-- Users table -------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.users
(
    id UInt32,
    username String,
    created_at Nullable(Datetime64(3)),
    ttl DATETIME DEFAULT now()
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_users
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_users TO bronze.users
(
    id UInt32,
    username String,
    created_at Nullable(Datetime64(3)),
    ttl Nullable(DateTime)
) AS SELECT
    id,
    trim(COALESCE(username, 'người dùng ẩn')) AS username,
    fromUnixTimestamp64Milli(created_at) AS created_at,
    now() as ttl
FROM bronze.kafka_users
WHERE id IS NOT NULL;


-- roles table ------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.roles
(
    id UInt32,
    role_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_roles
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_roles TO bronze.roles
(
    id UInt32,
    role_name String
) AS SELECT
    id,
    trim(lower(COALESCE(role_name, 'không rõ'))) AS role_name
FROM bronze.kafka_roles
WHERE id IS NOT NULL;

-- role_user table ----------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.role_user
(
    id UInt32,
    role_id UInt32,
    user_id UInt32
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_role_user
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_role_user TO bronze.role_user
(
    id UInt32,
    role_id UInt32,
    user_id UInt32
) AS SELECT
    id,
    role_id,
    user_id
FROM bronze.kafka_role_user
WHERE id IS NOT NULL;

-- provinces table --------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.provinces
(
    id UInt32,
    province_name LowCardinality(String)
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_provinces
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_provinces TO bronze.provinces
(
    id UInt32,
    province_name String
) AS SELECT
    id,
    trim(lower(COALESCE(province_name, 'không rõ'))) AS province_name
FROM bronze.kafka_provinces
WHERE id IS NOT NULL;

-- Cities table -------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.cities
(
    id UInt32,
    city_name LowCardinality(String),
    province_id UInt32,
    latitude Nullable(Float32),
    longitude Nullable(Float32)
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_cities
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_cities TO bronze.cities
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
FROM bronze.kafka_cities
WHERE id IS NOT NULL;

-- Addresses table ---------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.addresses
(
    id UInt32,
    title LowCardinality(String),
    user_id UInt32,
    province_id UInt32,
    city_id UInt32
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_addresses
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_addresses TO bronze.addresses
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
FROM bronze.kafka_addresses
WHERE id IS NOT NULL;

-- Categories Table ---------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.categories
(
    id UInt32,
    category_name LowCardinality(String),
    category_id Nullable(UInt32)
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_categories
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_categories TO bronze.categories
(
    id UInt32,
    category_name String,
    category_id Nullable(UInt32)
) AS SELECT
    id,
    trim(lower(COALESCE(category_name, 'chưa phân loại'))) AS category_name,
    category_id
FROM bronze.kafka_categories
WHERE id IS NOT NULL;

-- Product_Tag Table -------------------------------------
CREATE TABLE IF NOT EXISTS bronze.product_tag
(
    product_id UInt32,
    tag_id UInt32
) 
ENGINE = ReplacingMergeTree() -- Không cần key, tự động xóa theo Primary Key
ORDER BY (product_id, tag_id); -- Khóa chính là (product_id, tag_id)

CREATE TABLE IF NOT EXISTS bronze.kafka_product_tag
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_product_tag TO bronze.product_tag
(
    product_id UInt32,
    tag_id UInt32
) AS SELECT
    product_id,
    tag_id
FROM bronze.kafka_product_tag
WHERE product_id IS NOT NULL AND tag_id IS NOT NULL;

-- AdsCampaign Table ----------------------------------

CREATE TABLE IF NOT EXISTS bronze.adscampaigns
(
    id UInt32,
    campaign_title LowCardinality(String)
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_adscampaign
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_adscampaigns TO bronze.adscampaigns
(
    id UInt32,
    campaign_title String
) AS SELECT
    id,
    trim(COALESCE(campaign_title, 'không rõ')) AS campaign_title
FROM bronze.kafka_adscampaign
WHERE id IS NOT NULL;

-- Discounts Table -------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.discounts
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

CREATE TABLE IF NOT EXISTS bronze.kafka_discounts
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_discounts TO bronze.discounts
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
    fromUnixTimestamp64Milli(intDiv(started_at, 1000)) AS started_at,
    fromUnixTimestamp64Milli(intDiv(expired_at, 1000)) AS expired_at
FROM bronze.kafka_discounts
WHERE id IS NOT NULL;

-- OrderStatus Table -------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.orderstatus
(
	id UInt32,
	order_status_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_orderstatus
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderstatus TO bronze.orderstatus
(
    id UInt32,
    order_status_name String
) AS SELECT
    id,
    trim(COALESCE(order_status_name, 'Không rõ')) AS order_status_name
FROM bronze.kafka_orderstatus
WHERE id IS NOT NULL;

-- PaymentMethods Table -------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.paymentmethods
(
	id UInt32,
	payment_method_name String
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_paymentmethods
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_paymentmethods TO bronze.paymentmethods
(
    id UInt32,
    payment_method_name String
) AS SELECT
    id,
    trim(COALESCE(payment_method_name, 'Không rõ')) AS payment_method_name
FROM bronze.kafka_paymentmethods
WHERE id IS NOT NULL;

-- Payment Status Table-------------------------------------

CREATE TABLE IF NOT EXISTS bronze.paymentstatus
(
	id UInt32,
	payment_status_name String
) 
ENGINE = ReplacingMergeTree(id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_paymentstatus
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_paymentstatus TO bronze.paymentstatus
(
    id UInt32,
    payment_status_name String
) AS SELECT
    id,
    trim(COALESCE(payment_status_name, 'Không rõ')) AS payment_status_name
FROM bronze.kafka_paymentstatus
WHERE id IS NOT NULL;

-- Shipping Status Table-------------------------------------

CREATE TABLE IF NOT EXISTS bronze.shippingstatus
(
	id UInt32,
	shipping_status_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_shippingstatus
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_shippingstatus TO bronze.shippingstatus
(
    id UInt32,
    shipping_status_name String
) AS SELECT
    id,
    trim(COALESCE(shipping_status_name, 'Không rõ')) AS shipping_status_name
FROM bronze.kafka_shippingstatus
WHERE id IS NOT NULL;

-- Shipping Methods Table-------------------------------------

CREATE TABLE IF NOT EXISTS bronze.shippingmethods
(
	id UInt32,
	shipping_method_name String
) 
ENGINE = ReplacingMergeTree(id) 
ORDER BY (id);
CREATE TABLE IF NOT EXISTS bronze.kafka_shippingmethods
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


CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_shippingmethods TO bronze.shippingmethods
(
    id UInt32,
    shipping_method_name String
) AS SELECT
    id,
    trim(COALESCE(shipping_method_name, 'Không rõ')) AS shipping_method_name
FROM bronze.kafka_shippingmethods
WHERE id IS NOT NULL;

-- OrderDetails Table-------------------------------------

CREATE TABLE IF NOT EXISTS bronze.orderdetails
(
    id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity Nullable(Int32),
    product_price Nullable(Decimal(15, 2)),
    product_tax Nullable(Decimal(15, 2)),
    subtotal_amount Nullable(Decimal(15, 2)),
    created_at Datetime64(3) DEFAULT now()
) 
ENGINE = ReplacingMergeTree(id) 
PARTITION BY (toYYYYMM(created_at))
ORDER BY (id);

CREATE TABLE IF NOT EXISTS bronze.kafka_orderdetails
(
    id Nullable(UInt32),
    order_id Nullable(UInt32),
    product_id Nullable(UInt32),
    quantity Nullable(Int32),
    product_price Nullable(String),
    product_tax Nullable(String),
    subtotal_amount Nullable(String),
    created_at Nullable(Int64)
)
ENGINE Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:19092',
    kafka_topic_list = 'ecommerce_cdc.public.orderdetails',
    kafka_group_name = 'clickhouse_orderdetails_consumer',
    kafka_format = 'JSONEachRow', 
    kafka_num_consumers = 4, -- THAY ĐỔI: Tăng consumer
    kafka_skip_broken_messages = 10;


CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderdetails TO bronze.orderdetails
(
    id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity Nullable(Int32),
    product_price Nullable(Decimal(15, 2)),
    product_tax Nullable(Decimal(15, 2)),
    subtotal_amount Nullable(Decimal(15, 2)),
    created_at Nullable(Datetime64(3))
) AS SELECT
    id,
    order_id,
    product_id,
    quantity,
    toDecimal64OrNull(product_price, 2) AS product_price,
    toDecimal64OrNull(product_tax, 2) AS product_tax,
    toDecimal64OrNull(subtotal_amount, 2) AS subtotal_amount,
    fromUnixTimestamp64Milli(created_at) AS created_at 
FROM bronze.kafka_orderdetails
-- Lọc dữ liệu rác
WHERE
    id IS NOT NULL AND quantity > 0;


-- Testing dim fact
CREATE DATABASE IF NOT EXISTS analytics;

-- Category 
CREATE TABLE IF NOT EXISTS analytics.dim_category (
    category_id UInt32,
    category_name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY category_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dim_category TO analytics.dim_category
AS SELECT id, category_name, now64() 
FROM bronze.categories WHERE category_id IS NULL; 

-- Subcategory
CREATE TABLE IF NOT EXISTS analytics.subcategory (
    subcategory_id UInt32,
    category_id UInt32,
    subcategory_name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY subcategory_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_subcategory TO analytics.subcategory
AS SELECT id, category_id, category_name, now64() 
FROM bronze.categories WHERE category_id IS NOT NULL;

-- Brand
CREATE TABLE IF NOT EXISTS analytics.dim_brand (
    brand_id UInt32,
    brand_name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY brand_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dim_brand TO analytics.dim_brand
AS SELECT id, brand_name, now64() FROM bronze.brands;

-- Product
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key UInt32,
    product_name String,
    subcategory_id UInt32,
    brand_id UInt32,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY product_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dim_product TO analytics.dim_product
AS SELECT id, product_name, COALESCE(category_id, 0), COALESCE(brand_id, 0), now64() FROM bronze.products;

-- Date
CREATE TABLE IF NOT EXISTS analytics.dim_date
(
    date_key Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day UInt8,
    day_of_week UInt8,
    is_weekend UInt8
) ENGINE = MergeTree()
ORDER BY date_key;

-- nạp dữ liệu cho 10 năm
INSERT INTO analytics.dim_date
SELECT
    date,
    toYear(date) AS year,
    toQuarter(date) AS quarter,
    toMonth(date) AS month,
    toDayOfMonth(date) AS day,
    toDayOfWeek(date) AS day_of_week,
    if(toDayOfWeek(date) IN (6, 7), 1, 0) AS is_weekend
FROM (
    SELECT toDate('2025-10-01') + number AS date
    FROM numbers(3653)
);

-- Location
CREATE TABLE IF NOT EXISTS analytics.dim_location (
    city_name String,
    province_name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY city_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dim_location TO analytics.dim_location
AS SELECT c.city_name, p.province_name, now64()
FROM bronze.cities c JOIN bronze.provinces p ON c.province_id = p.id;

-- Campaign
CREATE TABLE IF NOT EXISTS analytics.dim_campaign (
    campaign_key UInt32,
    campaign_title String,
    start_at_key Date,
    expired_at_key Date,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY campaign_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_dim_campaign TO analytics.dim_campaign
AS SELECT a.id, a.campaign_title, toDate(d.started_at), toDate(d.expired_at), now64()
FROM bronze.adscampaigns a LEFT JOIN bronze.discounts d ON a.id = d.adscampaign_id;

-- Fact User
CREATE TABLE IF NOT EXISTS analytics.fact_user (
    date_key Date,
    user_count UInt64
) ENGINE = SummingMergeTree() ORDER BY date_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_user TO analytics.fact_user
AS SELECT toDate(created_at) AS date_key, count() AS user_count FROM bronze.users GROUP BY date_key;

-- Fact Product Campaign
CREATE TABLE IF NOT EXISTS analytics.fact_product_campaign (
    product_key UInt32,
    date_key Date,
    campaign_key UInt32,
    city_name String,
    quantity Int64,
    total_cost Decimal(18, 2),
    gmv_gross Decimal(18, 2),
    total_discount_val Decimal(18, 2),
    gmv_net Decimal(18, 2),
    order_count UInt64
) ENGINE = SummingMergeTree() ORDER BY (product_key, date_key, campaign_key, city_name);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_product_campaign TO analytics.fact_product_campaign
AS SELECT
    od.product_id AS product_key,
    toDate(od.created_at) AS date_key,
    COALESCE(o.discount_id, 0) AS campaign_key,
    COALESCE(addr.city_name, 'Unknown') AS city_name,
    sum(od.quantity) AS quantity,
    sum(od.quantity * p.unit_cost) AS total_cost,
    sum(od.quantity * od.product_price) AS gmv_gross,
    sum(o.discount_amount * (od.subtotal_amount / o.total_amount)) AS total_discount_val,
    sum(od.subtotal_amount) AS gmv_net,
    count(DISTINCT od.order_id) AS order_count
FROM bronze.orderdetails od
LEFT JOIN bronze.orders o ON od.order_id = o.id
LEFT JOIN bronze.products p ON od.product_id = p.id
LEFT JOIN (SELECT a.id, c.city_name FROM bronze.addresses a JOIN bronze.cities c ON a.city_id = c.id) addr ON o.address_id = addr.id
GROUP BY product_key, date_key, campaign_key, city_name;

-- Fact Order Shipping Status
CREATE TABLE IF NOT EXISTS analytics.fact_order_shipping_status (
    date_key Date,
    city_name String,
    campaign_key UInt32,
    -- Counts
    standard_shipping_order_count UInt64,
    express_shipping_order_count UInt64,
    next_day_delivery_order_count UInt64,
    free_shipping_order_count UInt64,
    store_pickup_order_count UInt64,
    -- GMV
    standard_shipping_gmv Decimal(18, 2),
    express_shipping_gmv Decimal(18, 2),
    next_day_delivery_gmv Decimal(18, 2),
    free_shipping_gmv Decimal(18, 2),
    store_pickup_gmv Decimal(18, 2),
    -- Status Counts
    pending_order_count UInt64,
    authorized_order_count UInt64,
    completed_order_count UInt64,
    failed_order_count UInt64,
    refunded_order_count UInt64
) ENGINE = SummingMergeTree() ORDER BY (date_key, city_name, campaign_key);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fact_order_shipping_status 
TO analytics.fact_order_shipping_status
AS SELECT
    toDate(o.created_at) AS date_key,
    COALESCE(addr.city_name, 'Unknown') AS city_name,
    COALESCE(o.discount_id, 0) AS campaign_key,
    
    -- map với shipping method
    countIf(o.shipping_method_id = 1) AS standard_shipping_order_count,
    countIf(o.shipping_method_id = 2) AS express_shipping_order_count,
    countIf(o.shipping_method_id = 3) AS next_day_delivery_order_count,
    countIf(o.shipping_method_id = 4) AS free_shipping_order_count,
    countIf(o.shipping_method_id = 5) AS store_pickup_order_count,
    
    sumIf(o.total_amount, o.shipping_method_id = 1) AS standard_shipping_gmv,
    sumIf(o.total_amount, o.shipping_method_id = 2) AS express_shipping_gmv,
    sumIf(o.total_amount, o.shipping_method_id = 3) AS next_day_delivery_gmv,
    sumIf(o.total_amount, o.shipping_method_id = 4) AS free_shipping_gmv,
    sumIf(o.total_amount, o.shipping_method_id = 5) AS store_pickup_gmv,
    
    -- map với order status
    countIf(o.order_status_id = 1) AS pending_order_count,      
    countIf(o.order_status_id = 2) AS authorized_order_count,   
    countIf(o.order_status_id = 4) AS completed_order_count,    
    countIf(o.order_status_id = 5) AS failed_order_count,       
    countIf(o.order_status_id = 3) AS shipped_order_count       
    
FROM bronze.orders o
LEFT JOIN (
    SELECT a.id, c.city_name 
    FROM bronze.addresses a 
    JOIN bronze.cities c ON a.city_id = c.id
) addr ON o.address_id = addr.id
GROUP BY date_key, city_name, campaign_key;