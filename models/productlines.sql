{{ config(materialized='table') }}

SELECT *
FROM {{ source('devstage', 'productlines') }};
