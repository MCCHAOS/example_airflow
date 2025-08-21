-- Populating Dimension: Dim_Products
TRUNCATE TABLE dwh."Dim_Product";

INSERT INTO dwh."Dim_Product" (
    "ProductID",
    "ProductCategory",
    "ProductNameLength",
    "ProductDescriptionLength",
    "ProductPhotosQty",
    "ProductWeight_g",
    "ProductLength_cm",
    "ProductHeight_cm",
    "ProductWidth_cm"
)
SELECT
    product_id,
    COALESCE(product_category, 'N/A'),
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM
    stage.product;