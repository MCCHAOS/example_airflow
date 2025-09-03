TRUNCATE TABLE dwh."Dim_Time";

WITH AllDates AS (
    SELECT DISTINCT TO_TIMESTAMP(order_date, 'MM/DD/YYYY HH24:MI')::DATE AS CalendarDate
    FROM stage.order
    WHERE
        order_date IS NOT NULL AND order_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    UNION
    SELECT DISTINCT TO_TIMESTAMP(order_approved_date, 'MM/DD/YYYY HH24:MI')::DATE
    FROM stage.order
    WHERE
        order_approved_date IS NOT NULL AND order_approved_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    UNION
    SELECT DISTINCT TO_TIMESTAMP(pickup_date, 'MM/DD/YYYY HH24:MI')::DATE
    FROM stage.order
    WHERE
        pickup_date IS NOT NULL AND pickup_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
    UNION
    SELECT DISTINCT TO_TIMESTAMP(delivered_date, 'MM/DD/YYYY HH24:MI')::DATE
    FROM stage.order
    WHERE
        delivered_date IS NOT NULL AND delivered_date ~ '^\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2}$'
)
INSERT INTO dwh."Dim_Time" ("TimeKey", "FullDate", "Year", "Quarter", "Month", "Day", "DayOfWeek")
SELECT
    -- Create an integer key from the date (e.g., 20250820)
    EXTRACT(YEAR FROM CalendarDate) * 10000 +
    EXTRACT(MONTH FROM CalendarDate) * 100 +
    EXTRACT(DAY FROM CalendarDate) AS "TimeKey",
    
    CalendarDate AS "FullDate",
    
    -- Extract date parts using EXTRACT()
    EXTRACT(YEAR FROM CalendarDate) AS "Year",
    EXTRACT(QUARTER FROM CalendarDate) AS "Quarter",
    EXTRACT(MONTH FROM CalendarDate) AS "Month",
    EXTRACT(DAY FROM CalendarDate) AS "Day",
    EXTRACT(DOW FROM CalendarDate) AS "DayOfWeek"
FROM AllDates
ON CONFLICT ("TimeKey") DO NOTHING;
