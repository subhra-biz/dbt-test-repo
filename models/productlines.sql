{{ config(
    materialized='incremental',  
    unique_key='dw_productline_id'     
) }}

-- Define the ETL batch number and batch date dynamically or statically
{% set etl_batch_no = 1001 %}
{% set etl_batch_date = '2001-01-01' %}

-- Define the logic for updating and inserting records


-- Insert new records (only if they do not exist in the target table)
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
    CAST({{ etl_batch_date }} AS DATE)
FROM devstage.productlines AS st
LEFT JOIN devdw.productlines AS dw
ON st.productLine = dw.productLine
WHERE dw.productLine IS NULL;
