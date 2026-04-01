with source as (
    select * from {{ source('erm_data', 'dbo_DEPOSITS') }}
),

cleaned as (
    select
        dep_invoiceno as invoice_number,
        dep_amount as amount,
        {{ clarion_date('dep_date') }} as deposit_date,
        {{ clarion_time('dep_time') }} as deposit_time,
        upper(trim(dep_type)) as payment_method,
        dep_stat as status,
        dep_custid as customer_id,
        dep_vatamount as vat_amount,
        dep_orderno as order_number

    from source
)

select * from cleaned
