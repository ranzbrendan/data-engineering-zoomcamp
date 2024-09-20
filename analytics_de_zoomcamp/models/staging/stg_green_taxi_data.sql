{{
    config(
        materialized='view'
    )
}}

with tripdata AS
(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY vendor_id, pickup_datetime) AS rn 
    FROM
        {{ source('staging', 'green_taxi_data')}}
    WHERE
        vendor_id IS NOT NULL
)

SELECT
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} AS trip_id,
    {{ dbt.safe_cast('vendor_id', api.Column.translate_type("integer")) }} AS vendor_id,
    {{ dbt.safe_cast('rate_code', api.Column.translate_type("integer")) }} AS rate_code,
    {{ dbt.safe_cast('pickup_location_id', api.Column.translate_type('integer')) }} AS pickup_location_id,
    {{ dbt.safe_cast('dropoff_location_id', api.Column.translate_type('integer')) }} AS dropoff_location_id,

    -- timestamps
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast('passenger_count', api.Column.translate_type('integer')) }} AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    {{ dbt.safe_cast('trip_type', api.Column.translate_type('integer')) }} AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(imp_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    COALESCE( {{ dbt.safe_cast('payment_type', api.Column.translate_type('integer')) }}, 0) AS payment_type,
    {{ get_payment_type_description('payment_type') }} AS payment_type_description 
FROM
    tripdata
WHERE
    rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
