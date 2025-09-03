TRUNCATE TABLE dwh."Fact_Payment";

INSERT INTO dwh."Fact_Payment" (
    "PaymentID",
    "OrderID",
    "PaymentSequential",
    "PaymentType",
    "PaymentInstallments",
    "PaymentValue"
)
SELECT
    -- Use ROW_NUMBER() to create a surrogate key
    ROW_NUMBER() OVER (ORDER BY order_id, payment_sequential) AS "PaymentID",
    order_id,
    payment_sequential,
    REPLACE(payment_type, '_', ' ') AS "PaymentType",
    payment_installments,
    payment_value
FROM
    stage.payment;
