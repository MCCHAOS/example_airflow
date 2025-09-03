TRUNCATE TABLE dwh."Fact_SalesLineItem";

INSERT INTO dwh."Fact_SalesLineItem" (
    "OrderID",
    "ProductID",
    "LineItemNumber",
    "UserID",
    "SellerID", 
    "TimeKey",
    "Price",
    "ShippingCost"    
)
SELECT
    oi.order_id,
    oi.product_id,
    oi.order_item_id AS "LineItemNumber",
    o.user_name AS "UserID",
    oi.seller_id,    
    -- Convert string date to integer TimeKey (YYYYMMDD)
    EXTRACT(YEAR FROM TO_TIMESTAMP(o.order_date, 'MM/DD/YYYY HH24:MI')) * 10000 +
    EXTRACT(MONTH FROM TO_TIMESTAMP(o.order_date, 'MM/DD/YYYY HH24:MI')) * 100 +
    EXTRACT(DAY FROM TO_TIMESTAMP(o.order_date, 'MM/DD/YYYY HH24:MI')) AS "TimeKey",
    oi.price,
    oi.shipping_cost    
FROM
    stage.order_item oi
JOIN
    stage.order o ON oi.order_id = o.order_id
WHERE
    o.order_date ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$';  
