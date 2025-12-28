INSERT INTO gold.FACT_SALES_PRODUCT
(
    date_key, location_key, campaign_key, product_key,
    quantity, gmv, total_cost, discount_val, net_revenue, order_count
)
SELECT
    toYYYYMMDD(o.created_at) AS date_key,
    o.province_id AS location_key,
    o.campaign_key AS campaign_key,
    i.product_id AS product_key,

    sum(i.quantity) AS quantity,
    sum(i.quantity * i.unit_price) AS gmv,
    sum(i.quantity * p.unit_cost) AS total_cost,

    sum(
        if(o.order_amount > 0, 
           ((i.quantity * i.unit_price) / o.order_amount) * o.discount_amount, 
           0)
    ) AS discount_val,

    sum(i.quantity * i.unit_price) - sum(
        if(o.order_amount > 0, 
           ((i.quantity * i.unit_price) / o.order_amount) * o.discount_amount, 
           0)
    ) AS net_revenue,

    uniq(o.order_id) AS order_count

FROM silver.order_items AS i
INNER JOIN silver.orders AS o ON i.order_id = o.order_id
LEFT JOIN silver.products AS p ON i.product_id = p.product_id

WHERE 
    o.order_status_id = 4 
    -- SỬA ĐỔI QUAN TRỌNG: Ép kiểu về múi giờ Việt Nam
    AND o.created_at >= toDateTime('{start_ts}', 'Asia/Ho_Chi_Minh')
    AND o.created_at <  toDateTime('{end_ts}', 'Asia/Ho_Chi_Minh')

GROUP BY 
    date_key, location_key, campaign_key, product_key;