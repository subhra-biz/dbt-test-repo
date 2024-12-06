{{ config(
    schema='devdw',
    materialized='incremental',  
    unique_key='productLine'     
) }}

-- Define batch variables
{% set etl_batch_no = var('etl_batch_no', '1') %}
{% set etl_batch_date = var('etl_batch_date', '2024-12-06') %}

-- Update existing records
WITH updated_records AS (
    SELECT 
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        CAST({{ etl_batch_no }} AS INTEGER) AS etl_batch_no,
        CAST({{ etl_batch_date }} AS DATE) AS etl_batch_date,
        st.productLine
    FROM {{ source('devstage', 'productlines') }} AS st
    JOIN {{ this }} AS dw
    ON st.productLine = dw.productLine
)
-- Apply updates
UPDATE {{ this }}
SET 
    src_update_timestamp = updated_records.src_update_timestamp,
    dw_update_timestamp = updated_records.dw_update_timestamp,
    etl_batch_no = updated_records.etl_batch_no,
    etl_batch_date = updated_records.etl_batch_date
FROM updated_records
WHERE {{ this }}.productLine = updated_records.productLine;

-- Insert new records
INSERT INTO {{ this }} (
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
    CAST({{ etl_batch_no }} AS INTEGER),
    CAST({{ etl_batch_date }} AS DATE)
FROM {{ source('devstage', 'productlines') }} AS st
LEFT JOIN {{ this }} AS dw
ON st.productLine = dw.productLine
WHERE dw.productLine IS NULL;

