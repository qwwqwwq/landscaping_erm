{{
    config(
        materialized='view'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

classified as (
    select
        customer_id,
        customer_code,
        customer_name,
        first_name,
        last_name,
        phone_1,
        phone_2,
        cell_phone,
        area_code_1,
        area_code_2,
        email,
        billing_address_1,
        billing_city,
        billing_state,
        billing_zip,
        customer_type,
        payment_terms,
        credit_limit,
        salesperson_code,
        created_date,

        -- House account: has credit limit or house/net payment terms
        case
            when credit_limit > 0 then true
            when upper(trim(payment_terms)) in ('HOUSE', 'NET10', 'NET15', 'NET30', 'NET60', 'NET90') then true
            else false
        end as is_house_account,

        -- Contractor detection via multiple signals
        case
            when customer_type = 'Business' then true
            when credit_limit > 0 then true
            when upper(trim(payment_terms)) in ('HOUSE', 'NET10', 'NET15', 'NET30', 'NET60', 'NET90') then true
            when regexp_contains(lower(customer_name), r'llc|inc\b|corp|land|garden|gardening|services|construction|contractor|scapes|scaping|paver|concrete')
                then true
            when regexp_contains(customer_name, r'&') then true
            when regexp_contains(lower(customer_name), r'\band\b') then true
            when regexp_contains(customer_name, r'[0-9]') then true
            else false
        end as is_contractor,

        -- Classification reason for transparency
        case
            when customer_type = 'Business' then 'Customer type: Business'
            when credit_limit > 0 then 'Has credit limit (house account)'
            when upper(trim(payment_terms)) in ('HOUSE', 'NET10', 'NET15', 'NET30', 'NET60', 'NET90')
                then 'House account payment terms: ' || trim(payment_terms)
            when regexp_contains(lower(customer_name), r'llc|inc\b|corp|land|garden|gardening|services|construction|contractor|scapes|scaping|paver|concrete')
                then 'Name pattern match (business keyword)'
            when regexp_contains(customer_name, r'&') then 'Name contains ampersand'
            when regexp_contains(lower(customer_name), r'\band\b') then 'Name contains "and"'
            when regexp_contains(customer_name, r'[0-9]') then 'Name contains numbers'
            else 'No contractor signals — classified as Retail'
        end as classification_reason,

        case
            when customer_type = 'Inactive' then 'Inactive'
            when customer_type = 'Business' then 'Contractor'
            when credit_limit > 0 then 'Contractor'
            when upper(trim(payment_terms)) in ('HOUSE', 'NET10', 'NET15', 'NET30', 'NET60', 'NET90') then 'Contractor'
            when regexp_contains(lower(customer_name), r'llc|inc\b|corp|land|garden|gardening|services|construction|contractor|scapes|scaping|paver|concrete')
                then 'Contractor'
            when regexp_contains(customer_name, r'&') then 'Contractor'
            when regexp_contains(lower(customer_name), r'\band\b') then 'Contractor'
            when regexp_contains(customer_name, r'[0-9]') then 'Contractor'
            else 'Retail'
        end as customer_segment

    from customers
)

select * from classified
