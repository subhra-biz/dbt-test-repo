{{ config(
    materialized='incremental',
    unique_key='de_productline_id'  
) }}

{% set etl_batch_no = 1001 %}
{% set etl_batch_date = '2001-01-01' %}

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
    COALESCE(dw.src_create_timestamp, st.create_timestamp) AS src_create_timestamp,
    st.update_timestamp AS src_update_timestamp,
    COALESCE(dw.dw_create_timestamp, CURRENT_TIMESTAMP) AS dw_create_timestamp,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    {{ etl_batch_no }} AS etl_batch_no,
    '{{ etl_batch_date }}'::DATE AS etl_batch_date
FROM {{ source('devstage', 'productlines') }} AS st
LEFT JOIN devdw.productlines AS dw
ON st.productLine = dw.productLine;
