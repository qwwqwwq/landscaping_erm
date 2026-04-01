-- NOTE: Password columns intentionally excluded for security
with source as (
    select * from {{ source('erm_data', 'dbo_SALESID') }}
),

cleaned as (
    select
        trim(sal_salesid) as employee_code,
        sal_salesnumber as employee_number,
        trim(sal_salesname) as employee_name,
        trim(sal_invname) as invoice_name,
        trim(sal_address) as address,
        trim(sal_citystzip) as city_state_zip,
        trim(sal_phone) as phone,
        trim(sal_cellnumber) as cell_phone,
        trim(sal_dept) as department,
        case sal_active
            when 1 then true
            when 0 then false
            when 2 then false  -- group/system accounts
            else false
        end as is_active,
        sal_salelimit as sale_limit,
        sal_returnlimit as return_limit,
        sal_register as register_number,
        sal_access as access_level,
        trim(sal_inorout) as in_or_out_status,
        trim(sal_goneto) as gone_to,
        trim(sal_backby) as back_by

    from source
)

select * from cleaned
