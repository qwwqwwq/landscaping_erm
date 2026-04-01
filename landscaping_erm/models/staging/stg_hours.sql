with source as (
    select * from {{ source('erm_data', 'dbo_HOURS') }}
),

cleaned as (
    select
        trim(hou_salesid) as employee_code,
        {{ clarion_date('hou_date') }} as work_date,
        {{ clarion_time('hou_timein') }} as time_in,
        {{ clarion_time('hou_timeout') }} as time_out,
        hou_type as hour_type,
        hou_invoiceno as invoice_number

    from source
)

select * from cleaned
