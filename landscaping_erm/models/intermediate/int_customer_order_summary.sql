{{
    config(
        materialized='view'
    )
}}

with customers as (
    select * from {{ ref('int_customer_classification') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where order_status not in ('V', 'VOID', 'X', 'CANCELLED')
),

order_summary as (
    select
        customer_id,
        count(*) as total_orders,
        sum(order_total) as lifetime_revenue,
        min(sales_date) as first_order_date,
        max(sales_date) as last_order_date,
        date_diff(current_date(), max(sales_date), day) as days_since_last_order,
        date_diff(current_date(), max(sales_date), month) as months_since_last_order,
        avg(order_total) as avg_order_value
    from orders
    where customer_id is not null
        and sales_date is not null
    group by customer_id
)

select
    c.customer_id,
    c.customer_code,
    c.customer_name,
    c.first_name,
    c.last_name,
    c.phone_1,
    c.phone_2,
    c.cell_phone,
    c.area_code_1,
    c.area_code_2,
    c.email,
    c.billing_address_1,
    c.billing_city,
    c.billing_state,
    c.billing_zip,
    c.customer_type,
    c.payment_terms,
    c.credit_limit,
    c.salesperson_code,
    c.created_date,
    c.is_house_account,
    c.is_contractor,
    c.classification_reason,
    c.customer_segment,

    coalesce(o.total_orders, 0) as total_orders,
    o.lifetime_revenue,
    o.first_order_date,
    o.last_order_date,
    o.days_since_last_order,
    o.months_since_last_order,
    o.avg_order_value,

    -- Useful date boundaries for attrition analysis
    date_sub(current_date(), interval 6 month) as cutoff_6_months,
    date_sub(current_date(), interval 1 year) as cutoff_1_year,
    date_sub(current_date(), interval 2 year) as cutoff_2_years,
    date_sub(current_date(), interval 5 year) as cutoff_5_years,
    date_sub(current_date(), interval 10 year) as cutoff_10_years

from customers c
left join order_summary o
    on c.customer_id = o.customer_id
