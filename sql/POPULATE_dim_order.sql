-- Populating Dimension: Dim_Orders
TRUNCATE TABLE dwh."Dim_Order";
INSERT INTO dwh."Dim_Order" (
    "OrderID",
    "OrderStatus",
    "OrderDate",
    "OrderApprovedDate",
    "PickupDate",
    "DeliveredDate",
    "EstimatedTimeDelivery"
)
SELECT
    order_id,
    order_status,
    TO_TIMESTAMP(order_date, 'MM/DD/YYYY HH24:MI'),
    TO_TIMESTAMP(order_approved_date, 'MM/DD/YYYY HH24:MI'),
    TO_TIMESTAMP(pickup_date, 'MM/DD/YYYY HH24:MI'),
    TO_TIMESTAMP(delivered_date, 'MM/DD/YYYY HH24:MI'),
    TO_TIMESTAMP(estimated_time_delivery, 'MM/DD/YYYY HH24:MI')
FROM
    stage.order
WHERE
    order_date IS NOT NULL
    AND order_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    AND order_approved_date IS NOT NULL
    AND order_approved_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    AND pickup_date IS NOT NULL
    AND pickup_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    AND delivered_date IS NOT NULL
    AND delivered_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    AND estimated_time_delivery IS NOT NULL
    AND estimated_time_delivery ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$';