{{ config(
    materialized='incremental',
    unique_key='productCode'
) }}

WITH updated_records AS (
    -- Update records that exist in both staging and destination tables
    SELECT 
        st.productCode,
        st.productName,
        st.productLine,
        st.productScale,
        st.productVendor,
        st.quantityInStock,
        st.buyPrice,
        st.MSRP,
        st.create_timestamp AS src_create_timestamp, -- Using create timestamp for updated records
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp, -- Set current timestamp for updated records
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        1001 AS etl_batch_no,
        CAST('2001-01-01' AS DATE) AS etl_batch_date
        
    FROM devstage.products AS st
    JOIN devdw.products AS dw
        ON st.productCode = dw.src_productCode
),
inserted_records AS (
    -- Insert new records from staging that do not exist in the destination table
    SELECT 
        st.productCode,
        st.productName,
        st.productLine,
        st.productScale,
        st.productVendor,
        st.quantityInStock,
        st.buyPrice,
        st.MSRP,
        st.create_timestamp AS src_create_timestamp, -- For new records, use create timestamp
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        1001 AS etl_batch_no,
        CAST('2001-01-01' AS DATE) AS etl_batch_date
    FROM devstage.products AS st
    LEFT JOIN devdw.products AS dw
        ON st.productCode = dw.src_productCode
    WHERE dw.src_productCode IS NULL
)

-- Combining both update and insert operations using UNION ALL
SELECT 
    productCode as src_productcode,
    productName,
    productLine,
    productScale,
    productVendor,
    quantityInStock,
    buyPrice,
    MSRP,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM updated_records

UNION ALL

SELECT 
    productCode as src_productcode,
    productName,
    productLine,
    productScale,
    productVendor,
    quantityInStock,
    buyPrice,
    MSRP,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM inserted_records

{% if is_incremental() %}
-- Only include rows that have been inserted or updated in incremental runs
WHERE productCode IS NOT NULL
{% endif %}
