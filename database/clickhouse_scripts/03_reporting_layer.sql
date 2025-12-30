CREATE DATABASE IF NOT EXISTS reporting;

-- =============================================
-- 1. VIEW CHO DASHBOARD: MARKETING
-- =============================================
DROP VIEW IF EXISTS reporting.view_marketing_dashboard;

CREATE VIEW reporting.view_marketing_dashboard AS
SELECT
    d.full_date AS date,
    d.month AS month,
    d.quarter AS quarter,
    d.year AS year,
    l.province_name AS province,
    l.region_name AS region,
    p.category_name AS parent_category,
    p.subcategory_name AS subcategory,
    p.product_name AS product_name,    
    c.campaign_title AS campaign_title,
    f.gmv AS total_gmv,
    f.net_revenue AS total_net_revenue,
    f.quantity AS quantity_sold,
    f.order_count AS order_count,
    if(f.order_count > 0, f.gmv / f.order_count, 0) AS aov,
    if(f.quantity > 0, f.gmv / f.quantity, 0) AS avg_product_price
FROM gold.FACT_SALES_PRODUCT AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
LEFT JOIN gold.dim_locations AS l ON f.location_key = l.location_key
LEFT JOIN gold.dim_products AS p ON f.product_key = p.product_key
LEFT JOIN gold.dim_campaigns AS c ON f.campaign_key = c.campaign_key;

-- =============================================
-- 2. VIEWS CHO DASHBOARD: OVERVIEW
-- =============================================

-- 2.1 View Tổng Quan Bán Hàng
DROP VIEW IF EXISTS reporting.view_overview_sales;

CREATE VIEW reporting.view_overview_sales AS
SELECT
    d.full_date AS date,
    d.month AS month,
    d.year AS year,
    l.province_name AS province,
    p.product_name AS product,
    p.category_name AS category,
    f.gmv AS total_gmv,
    f.net_revenue AS total_net_revenue,
    f.quantity AS total_quantity_sold,
    f.order_count AS order_count
FROM gold.FACT_SALES_PRODUCT AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
LEFT JOIN gold.dim_locations AS l ON f.location_key = l.location_key
LEFT JOIN gold.dim_products AS p ON f.product_key = p.product_key;

-- 2.2 View Phân Tích Đơn Hàng
DROP VIEW IF EXISTS reporting.view_overview_orders;

CREATE VIEW reporting.view_overview_orders AS
SELECT
    d.full_date AS date,
    d.month AS month,
    l.province_name AS province,
    s.name AS order_status,
    f.payment_method AS payment_method,
    f.shipping_method AS shipping_method,
    f.order_count AS order_count,
    f.total_gmv AS order_revenue
FROM gold.FACT_ORDER_OVERVIEW AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
LEFT JOIN gold.dim_locations AS l ON f.location_key = l.location_key
LEFT JOIN gold.dim_order_status AS s ON f.order_status_id = s.id;

-- 2.3 View User Đăng Ký
DROP VIEW IF EXISTS reporting.view_overview_users;

CREATE VIEW reporting.view_overview_users AS
SELECT
    d.full_date AS registration_date,
    d.month AS month,
    sum(f.user_amount) AS new_user_count
FROM gold.FACT_USER_REGISTRATION AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
GROUP BY registration_date, month;