TRUNCATE TABLE dwh."Dim_Seller";

INSERT INTO dwh."Dim_Seller" (
    "SellerID",
    "SellerZIPCode",
    "SellerCity",
    "SellerState"
)
SELECT DISTINCT
    seller_id,
    seller_zip_code,
    UPPER(seller_city),
    UPPER(seller_state)
FROM
    stage.seller;