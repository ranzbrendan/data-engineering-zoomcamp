{#
    This macro returns the description of the payment_type
#}

{% macro get_payment_type_description(payment_type)-%}

    CASE CAST(REPLACE( {{ payment_type }},'.0','') AS INTEGER)
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        else 'EMPTY'
    END

{%- endmacro %}