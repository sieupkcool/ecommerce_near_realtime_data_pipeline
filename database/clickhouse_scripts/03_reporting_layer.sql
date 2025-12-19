CREATE DATABASE IF NOT EXISTS reporting;

-- =============================================
-- 1. VIEW CHO DASHBOARD: MARKETING
-- =============================================
DROP VIEW IF EXISTS reporting.view_marketing_dashboard;

CREATE VIEW reporting.view_marketing_dashboard AS
SELECT
    d.full_date AS "Ngày",
    d.month AS "Tháng",
    d.quarter AS "Quý",
    d.year AS "Năm",
    l.city_name AS "Thành Phố",
    l.province_name AS "Khu Vực",
    p.category_name AS "Danh Mục Cha",
    p.subcategory_name AS "Danh Mục Con",
    p.product_name AS "Tên Sản Phẩm",    

    c.campaign_title AS "Tên Chiến Dịch",
    
    f.gmv AS "Tổng GMV",
    f.net_revenue AS "Tổng Net Revenue",
    f.quantity AS "Số Lượng Bán",
    f.order_count AS "Số Đơn Hàng",
    
    if(f.order_count > 0, f.gmv / f.order_count, 0) AS "AOV (Giá Trị Đơn TB)",
    if(f.quantity > 0, f.gmv / f.quantity, 0) AS "Giá Bán TB Sản Phẩm"

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
    d.full_date AS "Ngày",
    d.month AS "Tháng",
    d.year AS "Năm",
    l.city_name AS "Thành Phố",
    p.product_name AS "Sản Phẩm",
    p.category_name AS "Danh Mục",
    
    f.gmv AS "Tổng GMV",
    f.net_revenue AS "Tổng Net Revenue",
    f.quantity AS "Tổng Số Sản Phẩm Bán Ra",
    f.order_count AS "Số Đơn Hàng"

FROM gold.FACT_SALES_PRODUCT AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
LEFT JOIN gold.dim_locations AS l ON f.location_key = l.location_key
LEFT JOIN gold.dim_products AS p ON f.product_key = p.product_key;

-- 2.2 View Phân Tích Đơn Hàng
DROP VIEW IF EXISTS reporting.view_overview_orders;

CREATE VIEW reporting.view_overview_orders AS
SELECT
    -- 1. Thời gian & Địa điểm
    d.full_date AS "Ngày",
    d.month AS "Tháng",
    l.city_name AS "Thành Phố",
    
    -- 2. Trạng Thái (Lấy từ bảng Dim thông qua JOIN)    
    s.name AS "Trạng Thái Đơn Hàng",
    
    -- 3. Thanh Toán & Vận Chuyển (Lấy TRỰC TIẾP từ Fact)
    f.payment_method AS "Phương Thức Thanh Toán",
    f.shipping_method AS "Hình Thức Vận Chuyển",
    
    -- 4. Số liệu
    f.order_count AS "Số Lượng Đơn",
    f.total_gmv AS "Doanh Thu Đơn Hàng"

FROM gold.FACT_ORDER_OVERVIEW AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
LEFT JOIN gold.dim_locations AS l ON f.location_key = l.location_key
--
LEFT JOIN gold.dim_order_status AS s ON f.order_status_id = s.id;

-- 2.3 View User Đăng Ký
DROP VIEW IF EXISTS reporting.view_overview_users;

CREATE VIEW reporting.view_overview_users AS
SELECT
    d.full_date AS "Ngày Đăng Ký",
    d.month AS "Tháng",
    count() AS "Số User Mới Đăng Ký"
FROM gold.FACT_USER_REGISTRATION AS f
LEFT JOIN gold.dim_date AS d ON f.date_key = d.date_key
GROUP BY "Ngày Đăng Ký", "Tháng";