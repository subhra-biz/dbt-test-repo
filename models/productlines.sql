{{ config(
    materialized='incremental',
    unique_key='productLine'  
) }}

{% set etl_batch_no = 1001 %}
{% set etl_batch_date = '2001-01-01' %}

-- Step 1: Update existing records
WITH updated_records AS (
    SELECT 
        st.productLine,
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        CAST({{ etl_batch_no }} AS INTEGER) AS etl_batch_no,
        CAST('{{ etl_batch_date }}' AS DATE) AS etl_batch_date
    FROM devstage.productlines AS st
    INNER JOIN devdw.productlines AS dw
    ON st.productLine = dw.productLine
)
UPDATE devdw.productlines
SET 
    src_update_timestamp = updated_records.src_update_timestamp,
    dw_update_timestamp = updated_records.dw_update_timestamp,
    etl_batch_no = updated_records.etl_batch_no,
    etl_batch_date = updated_records.etl_batch_date
FROM updated_records
WHERE devdw.productlines.productLine = updated_records.productLine;

-- Step 2: Insert new records
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
    CAST({{ etl_batch_no }} AS INTEGER),
    CAST('{{ etl_batch_date }}' AS DATE)
FROM devstage.productlines AS st
LEFT JOIN devdw.productlines AS dw
ON st.productLine = dw.productLine
WHERE dw.productLine IS NULL;
