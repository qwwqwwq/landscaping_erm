with source as (
    select * from {{ source('erm_data', 'dbo_CHECKS') }}
),

cleaned as (
    select
        trim(che_name) as payer_name,
        che_amount as amount,
        {{ clarion_date('che_date') }} as check_date,
        trim(che_number) as check_number,
        trim(che_dlnmbr) as drivers_license_number,
        trim(che_desc) as description,
        che_custid as customer_id,
        case upper(trim(che_type))
            when 'I' then 'Invoice'
            when 'A' then 'AR Payment'
            when 'O' then 'Order'
            when 'D' then 'Deposit'
            when 'P' then 'Prepayment'
            else upper(trim(che_type))
        end as check_type,
        che_invoiceno as invoice_number

    from source
)

select * from cleaned
