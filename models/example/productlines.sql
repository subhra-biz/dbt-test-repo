{{ config(
    schema='devdw',        -- Target schema
    materialized='table'   -- Ensures the output is a table in Redshift
) }}

SELECT *
FROM {{ source('devstage', 'productlines') }}; -- Source table in devstage schema
