-- Populating Dimension: Dim_Users
TRUNCATE TABLE dwh."Dim_User";
INSERT INTO dwh."Dim_User" ("UserID", "UserZIPCode", "UserCity", "UserState")
SELECT DISTINCT
user_name,
customer_zip_code,
UPPER(customer_city),
UPPER(customer_state)
FROM stage.user;