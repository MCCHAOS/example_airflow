TRUNCATE TABLE dwh."Fact_Sales";

INSERT INTO dwh."Fact_Sales" (
    "OrderID",
    "ProductID",
    "UserID",
    "SellerID",
    "PaymentID",
    "FeedbackID",
    "TimeKey",
    "Price",
    "ShippingCost",
    "PaymentValue"
)
SELECT
    oi.order_id,
    oi.product_id,
    o.user_name AS "UserID",
    oi.seller_id,
    p."PaymentID",
    f.feedback_id,
    -- Convert string date to integer TimeKey (YYYYMMDD)
    EXTRACT(YEAR FROM TO_TIMESTAMP(o.order_date, 'MM/DD/YYYY HH24:MI')) * 10000 +
    EXTRACT(MONTH FROM TO_TIMESTAMP(o.order_date, 'MM/DD/YYYY HH24:MI')) * 100 +
    EXTRACT(DAY FROM TO_TIMESTAMP(o.order_date, 'MM/DD/YYYY HH24:MI')) AS "TimeKey",
    oi.price,
    oi.shipping_cost,
    p."PaymentValue"
FROM
    stage.order_item oi
JOIN
    stage.order o ON oi.order_id = o.order_id
LEFT JOIN
    -- Join with the actual dimension table
    dwh."Dim_Payment" p ON oi.order_id = p."OrderID"
LEFT JOIN
    -- Join for feedback
    stage.feedback f ON oi.order_id = f.order_id
WHERE
    p."PaymentSequential" = 1;
