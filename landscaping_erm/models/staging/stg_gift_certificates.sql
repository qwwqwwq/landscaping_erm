with source as (
    select * from {{ source('erm_data', 'dbo_GIFTCERT') }}
),

cleaned as (
    select
        gif_sysid as gift_certificate_id,
        trim(gif_giftnumber) as gift_number,
        gif_amount as original_amount,
        gif_spent as spent_amount,
        gif_amount - gif_spent as remaining_balance,
        gif_salesnumber as sales_number,
        gif_invoiceno as invoice_number,
        {{ clarion_date('gif_date') }} as issue_date,
        {{ clarion_date('gif_lastused') }} as last_used_date

    from source
)

select * from cleaned
