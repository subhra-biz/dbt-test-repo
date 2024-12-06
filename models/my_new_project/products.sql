{{ config(
    materialized='incremental',  
    unique_key='src_productCode'  
) }}

WITH updated_records AS (
    SELECT 
        st.productName,
        st.productLine,
        st.productScale,
        st.productVendor,
        st.quantityInStock,
        st.buyPrice,
        st.MSRP,
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        1001 AS etl_batch_no,  -- Static value for ETL batch number
        '2001-01-01'::DATE AS etl_batch_date,  -- Static value for batch date
        st.productCode
    FROM devstage.products AS st
    JOIN devdw.products AS dw
    ON st.productCode = dw.src_productCode
),

inserted_records AS (
    SELECT 
        st.productCode,
        st.productName,
        st.productLine,
        st.productScale,
        st.productVendor,
        st.quantityInStock,
        st.buyPrice,
        st.MSRP,
        st.create_timestamp AS src_create_timestamp,
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        1001 AS etl_batch_no,  -- Static value for ETL batch number
        '2001-01-01'::DATE AS etl_batch_date  -- Static value for batch date
    FROM devstage.products AS st
    LEFT JOIN devdw.products AS dw
    ON st.productCode = dw.src_productCode
    WHERE dw.src_productCode IS NULL
)

-- Final SELECT to combine both UPDATE and INSERT logic
SELECT * FROM updated_records

{% if is_incremental() %}
UNION ALL
SELECT * FROM inserted_records
{% endif %}
