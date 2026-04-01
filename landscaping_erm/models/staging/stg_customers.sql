with source as (
    select * from {{ source('erm_data', 'dbo_CUSMER') }}
),

cleaned as (
    select
        cus_custid as customer_id,
        trim(cus_code) as customer_code,
        trim(cus_name) as customer_name,
        trim(cus_fnam) as first_name,
        trim(cus_lnam) as last_name,
        trim(cus_phn1) as phone_1,
        trim(cus_phn2) as phone_2,
        trim(cus_cellphone) as cell_phone,
        trim(cus_areacode1) as area_code_1,
        trim(cus_areacode2) as area_code_2,
        trim(cus_bad1) as billing_address_1,
        trim(cus_bad2) as billing_address_2,
        trim(cus_bad3) as billing_address_3,
        trim(cus_bcit) as billing_city,
        trim(cus_bsta) as billing_state,
        trim(cus_bzip) as billing_zip,
        trim(cus_bzp4) as billing_zip4,
        trim(cus_email) as email,
        trim(cus_title) as title,
        case upper(trim(cus_type))
            when 'P' then 'Personal'
            when 'B' then 'Business'
            when 'Z' then 'Inactive'
            else upper(trim(cus_type))
        end as customer_type,
        upper(trim(cus_term)) as payment_terms,
        cus_clim as credit_limit,
        cus_taxid as tax_id,
        cus_shipid as ship_to_id,
        trim(cus_priceid) as price_level_id,
        cus_permdisc as permanent_discount_pct,
        trim(cus_salp) as salesperson_code,
        cus_defid as default_form_id,
        cus_parentid as parent_customer_id,
        cus_groupid as group_id,
        cus_credithold as credit_hold_flag,
        cus_orderhold as order_hold_amount,
        cus_accounthold as account_hold_amount,
        cus_ar_id as ar_id,
        cus_allowsmstextmessages as allow_sms_flag,
        cus_emailstatements as email_statements_flag,
        {{ clarion_date('cus_createddate') }} as created_date,
        {{ clarion_date('cus_dateofbirth') }} as date_of_birth

    from source
)

select * from cleaned
