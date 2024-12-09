{{ config(
    materialized='incremental',
    unique_key='productline'
) }}

WITH processed_data AS (
    SELECT
        st.productline,
        '' AS textdescription,
        COALESCE(dw.src_create_timestamp, st.create_timestamp) AS src_create_timestamp,
        COALESCE(st.update_timestamp, dw.src_update_timestamp) AS src_update_timestamp,
        1001 AS etl_batch_no,
        TO_DATE('2001-01-01', 'YYYY-MM-DD') AS etl_batch_date,
        COALESCE(dw.dw_update_timestamp, CURRENT_TIMESTAMP) AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        ROW_NUMBER() OVER (ORDER BY st.productline) 
            + COALESCE(MAX(dw.dw_product_line_id) OVER (), 0) AS dw_product_line_id
    FROM 
        devstage.productlines AS st
    LEFT JOIN devdw.productlines AS dw
        ON st.productline = dw.productline
)

SELECT *
FROM processed_data

{% if is_incremental() %}
WHERE 
    processed_data.src_update_timestamp > devdw.productlines.dw_update_timestamp
    OR devdw.productlines.productline IS NULL  -- Handle new and updated rows only
{% endif %}
