{{
    config(
        materialized='table'
    )
}}

SELECT
    {{ dbt.safe_cast('locationid', api.Column.translate_type('integer')) }} AS location_id,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') AS service_zone
FROM 
    {{ ref('taxi_zone_lookup') }}