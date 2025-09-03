-- Find number of orders in seasons to find Peak Season
---------------------------------------------------------------------
SELECT    
    CASE -- to extract the season from the OrderDate
        WHEN EXTRACT(MONTH FROM "OrderDate") IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM "OrderDate") IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM "OrderDate") IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS "Season",
    COUNT("OrderID") AS "TotalOrders"
FROM
    "dwh"."Dim_Order"
GROUP BY
    "Season"
ORDER BY
    "TotalOrders" DESC;

---------------------------------------------------------------------

-- Find peak month by count of orders
SELECT
    EXTRACT(MONTH FROM "OrderDate") AS "MonthNumber",
    TO_CHAR("OrderDate", 'Month') AS "MonthName",
    COUNT("OrderID") AS "TotalOrders"
FROM
    "dwh"."Dim_Order"
GROUP BY
    "MonthNumber", "MonthName"
ORDER BY
    "TotalOrders" DESC;

---------------------------------------------------------------------

-- Find which payment type is most preferred based on number of payments using that type
SELECT
    "PaymentType",
    COUNT(*) AS "NumberOfPayments",
    -- Calculate the percentage of total payments and round to 2 decimal places
    ROUND(
        (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM "dwh"."Fact_Payment")),
        2
    ) AS "PercentageOfTotal"
FROM
    "dwh"."Fact_Payment"
GROUP BY
    "PaymentType"
ORDER BY
    "NumberOfPayments" DESC;



