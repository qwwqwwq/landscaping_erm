{{
    config(
        materialized='table'
    )
}}

with customer_orders as (
    select * from {{ ref('int_customer_order_summary') }}
    where customer_segment = 'Contractor'
        and total_orders > 0
),

attrition as (
    select
        *,

        -- Report 1: Active in last 10 years, no order in past 1 year
        case
            when first_order_date >= cutoff_10_years
                and last_order_date < cutoff_1_year
                then true
            else false
        end as attrition_10yr_inactive_1yr,

        -- Report 2a: Active in last 5 years, no order in past 6 months
        case
            when first_order_date >= cutoff_5_years
                and last_order_date < cutoff_6_months
                then true
            else false
        end as attrition_5yr_inactive_6mo,

        -- Report 2b: Active in last 5 years, no order in past 1 year
        case
            when first_order_date >= cutoff_5_years
                and last_order_date < cutoff_1_year
                then true
            else false
        end as attrition_5yr_inactive_1yr,

        -- Report 2c: Active in last 5 years, no order in past 2 years
        case
            when first_order_date >= cutoff_5_years
                and last_order_date < cutoff_2_years
                then true
            else false
        end as attrition_5yr_inactive_2yr,

        -- Best available phone number for outreach
        coalesce(
            nullif(
                case
                    when cell_phone is not null and cell_phone != ''
                        then cell_phone
                end, ''),
            nullif(
                case
                    when phone_1 is not null and phone_1 != ''
                        then concat(coalesce(area_code_1, ''), phone_1)
                end, ''),
            nullif(
                case
                    when phone_2 is not null and phone_2 != ''
                        then concat(coalesce(area_code_2, ''), phone_2)
                end, '')
        ) as best_phone_number

    from customer_orders
)

select
    customer_id,
    customer_code,
    customer_name,
    first_name,
    last_name,
    best_phone_number,
    phone_1,
    phone_2,
    cell_phone,
    email,
    billing_city,
    billing_state,
    customer_type,
    is_house_account,
    classification_reason,
    salesperson_code,
    total_orders,
    lifetime_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    days_since_last_order,
    months_since_last_order,
    attrition_10yr_inactive_1yr,
    attrition_5yr_inactive_6mo,
    attrition_5yr_inactive_1yr,
    attrition_5yr_inactive_2yr

from attrition
where attrition_10yr_inactive_1yr
    or attrition_5yr_inactive_6mo
    or attrition_5yr_inactive_1yr
    or attrition_5yr_inactive_2yr

order by lifetime_revenue desc
