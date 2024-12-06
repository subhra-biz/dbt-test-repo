{{ config(
    materialized='incremental'
) }}

WITH source_data AS (
    SELECT
        productcode AS src_productcode,
        productname,
        productline,
        productscale,
        productvendor,
        quantityinstock,
        buyprice,
        msrp,
        create_timestamp,
        update_timestamp,
        1001 AS etl_batch_no,
        TO_DATE('2001-01-01', 'YYYY-MM-DD') AS etl_batch_date
    FROM
        devstage.products
),

product_lines AS (
    SELECT
        productline,
        dw_product_line_id
    FROM
        devdw.productlines
),

existing_data AS (
    SELECT
        src_productcode,
        productname,
        productline,
        productscale,
        productvendor,
        quantityinstock,
        buyprice,
        msrp,
        dw_product_line_id,
        src_create_timestamp,
        src_update_timestamp,
        etl_batch_no,
        etl_batch_date,
        dw_update_timestamp,
        dw_product_id
    FROM
        devdw.products  -- Refers to the current state of the table created by dbt
),

ranked_data AS (
    SELECT
        source_data.src_productcode,
        source_data.productname,
        source_data.productline,
        source_data.productscale,
        source_data.productvendor,
        source_data.quantityinstock,
        source_data.buyprice,
        source_data.msrp,
        COALESCE(pl.dw_product_line_id, existing_data.dw_product_line_id) AS dw_product_line_id,
        CASE
            WHEN existing_data.src_productcode IS NULL THEN source_data.create_timestamp
            ELSE existing_data.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(source_data.update_timestamp, existing_data.src_update_timestamp) AS src_update_timestamp,
        1001 AS etl_batch_no,
        TO_DATE('2001-01-01', 'YYYY-MM-DD') AS etl_batch_date,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CASE
            WHEN source_data.src_productcode IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE existing_data.dw_update_timestamp
        END AS dw_update_timestamp,
        ROW_NUMBER() OVER (ORDER BY source_data.src_productcode) + COALESCE(MAX(existing_data.dw_product_id) OVER (), 0) AS dw_product_id
    FROM
        source_data
    LEFT JOIN existing_data ON source_data.src_productcode = existing_data.src_productcode
    LEFT JOIN product_lines pl ON source_data.productline = pl.productline
)

SELECT *
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.src_productcode IS NOT NULL  -- Only process new or updated rows
{% endif %}