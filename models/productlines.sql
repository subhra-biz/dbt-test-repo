{{ config(
    materialized='incremental',
    unique_key='productLine',
    incremental_strategy='merge'
) }}

WITH updated_records AS (
    SELECT 
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        CAST(1 AS INTEGER) AS etl_batch_no,
        CAST('2024-12-06' AS DATE) AS etl_batch_date,
        st.productLine
    FROM devstage.productlines AS st
    JOIN devdw.productlines AS dw
    ON st.productLine = dw.productLine
)
-- Update existing records
UPDATE devdw.productlines
SET 
    src_update_timestamp = updated_records.src_update_timestamp,
    dw_update_timestamp = updated_records.dw_update_timestamp,
    etl_batch_no = updated_records.etl_batch_no,
    etl_batch_date = updated_records.etl_batch_date
FROM updated_records
WHERE devdw.productlines.productLine = updated_records.productLine

{% if is_incremental() %}
    AND updated_records.productLine IS NOT NULL
{% endif %}

-- Insert new records
INSERT INTO devdw.productlines (
    productLine,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
)
SELECT 
    st.productLine,
    st.create_timestamp,
    st.update_timestamp,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CAST(1 AS INTEGER),
    CAST('2024-12-06' AS DATE)
FROM devstage.productlines AS st
LEFT JOIN devdw.productlines AS dw
ON st.productLine = dw.productLine
WHERE dw.productLine IS NULL

{% if is_incremental() %}
    AND st.productLine IS NOT NULL
{% endif %}
