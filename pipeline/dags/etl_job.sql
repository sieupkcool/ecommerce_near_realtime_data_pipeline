INSERT INTO gold.FACT_SALES_PRODUCT
(
    date_key, location_key, campaign_key, product_key,
    quantity, gmv, total_cost, discount_val, net_revenue, order_count
)
SELECT
    toYYYYMMDD(o.created_at) AS date_key,
    o.city_id AS location_key,
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
    AND o.created_at >= '{start_ts}' 
    AND o.created_at < '{end_ts}'

GROUP BY 
    date_key, location_key, campaign_key, product_key;