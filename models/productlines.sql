{{ config(
    materialized='incremental',  
    unique_key='dw_productline_id'  
) }}

{% set etl_batch_no = 1001 %}
{% set etl_batch_date = '2001-01-01' %}

-- Use a single UPSERT operation (MERGE equivalent in Redshift)
MERGE INTO devdw.productlines AS target
USING devstage.productlines AS source
ON target.productLine = source.productLine
WHEN MATCHED THEN
    UPDATE SET
        target.src_update_timestamp = source.update_timestamp,
        target.dw_update_timestamp = CURRENT_TIMESTAMP,
        target.etl_batch_no = CAST({{ etl_batch_no }} AS INTEGER),
        target.etl_batch_date = CAST({{ etl_batch_date }} AS DATE)
WHEN NOT MATCHED THEN
    INSERT (
        productLine,
        src_create_timestamp,
        src_update_timestamp,
        dw_create_timestamp,
        dw_update_timestamp,
        etl_batch_no,
        etl_batch_date
    )
    VALUES (
        source.productLine,
        source.create_timestamp,
        source.update_timestamp,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        CAST({{ etl_batch_no }} AS INTEGER),
        CAST({{ etl_batch_date }} AS DATE)
    );
