-- File: etl_job.sql
-- Nhiệm vụ: Đổ dữ liệu từ Silver vào Gold (FACT_SALES_PRODUCT) theo cơ chế Incremental Load
-- Chạy định kỳ bởi container etl-worker

INSERT INTO gold.FACT_SALES_PRODUCT
(
    date_key,
    location_key,
    campaign_key,
    product_key,
    quantity,
    gmv,
    total_cost,
    discount_val,
    net_revenue,
    order_count
)
SELECT
    -- 1. CÁC KHÓA DIMENSION (Grouping Keys)
    toYYYYMMDD(o.created_at) AS date_key,
    o.city_id AS location_key,       -- Đã lookup từ silver_orders
    o.campaign_key AS campaign_key,  -- Đã lookup từ silver_orders
    i.product_id AS product_key,

    -- 2. CÁC METRICS CƠ BẢN
    sum(i.quantity) AS quantity,
    
    -- GMV (Doanh thu thô) = Số lượng * Giá bán
    sum(i.quantity * i.unit_price) AS gmv,
    
    -- Total Cost (Giá vốn) = Số lượng * Giá vốn (Lấy từ bảng Products)
    sum(i.quantity * p.unit_cost) AS total_cost,

    -- 3. CÁC METRICS TÍNH TOÁN PHỨC TẠP (PHÂN BỔ)
    
    -- Discount Value (Giảm giá phân bổ cho từng dòng sản phẩm)
    -- Công thức: (GMV dòng / Tổng tiền hàng đơn) * Tổng giảm giá đơn
    -- Lưu ý: Dùng order_amount (Subtotal) làm mẫu số là chuẩn nhất
    sum(
        if(o.order_amount > 0, 
           ((i.quantity * i.unit_price) / o.order_amount) * o.discount_amount, 
           0)
    ) AS discount_val,

    -- Net Revenue (Doanh thu thuần) = GMV - Discount đã phân bổ
    sum(i.quantity * i.unit_price) - sum(
        if(o.order_amount > 0, 
           ((i.quantity * i.unit_price) / o.order_amount) * o.discount_amount, 
           0)
    ) AS net_revenue,

    -- 4. SỐ LƯỢNG ĐƠN HÀNG
    uniq(o.order_id) AS order_count

FROM silver.order_items AS i
-- Join Header để lấy thông tin đơn hàng (Địa điểm, Campaign, Discount tổng)
INNER JOIN silver.orders AS o ON i.order_id = o.order_id
-- Join Product để lấy giá vốn (Unit Cost) mà trong order_items không có
LEFT JOIN silver.products AS p ON i.product_id = p.product_id

WHERE 
    -- 1. Chỉ tính doanh thu cho đơn hàng THÀNH CÔNG (Status = 4 : Delivered)
    o.order_status_id = 4 
    
    -- 2. INCREMENTAL LOAD (Gối đầu thời gian)
    -- Lấy các đơn hàng vừa được cập nhật trong 15 phút gần nhất.
    -- Job chạy 5 phút/lần -> Lấy 15 phút để đảm bảo không bị sót đơn do trễ mạng.
    AND o.updated_at >= now() - INTERVAL 15 MINUTE

GROUP BY 
    date_key, 
    location_key, 
    campaign_key, 
    product_key;