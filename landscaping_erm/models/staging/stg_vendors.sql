with source as (
    select * from {{ source('erm_data', 'dbo_APVEND') }}
),

cleaned as (
    select
        ven_v_id as vendor_id,
        trim(ven_o_id) as vendor_code,
        trim(ven_name) as vendor_name,
        trim(ven_addr) as address_1,
        trim(ven_adr2) as address_2,
        trim(ven_city) as city,
        trim(ven_stat) as state,
        trim(ven_zipc) as zip_code,
        trim(ven_coun) as country,
        trim(ven_phon) as phone,
        trim(ven_faxn) as fax,
        trim(ven_cont) as contact_1_name,
        trim(ven_c1ph) as contact_1_phone,
        trim(ven_con2) as contact_2_name,
        trim(ven_c2ph) as contact_2_phone,
        trim(ven_is1099) as is_1099_vendor,
        ven_clmt as credit_limit,
        trim(ven_taxtable) as tax_table,
        trim(ven_taxidnumber) as tax_id_number,
        {{ clarion_date('ven_taxidexpiration') }} as tax_id_expiration_date,
        ven_buyergroup as buyer_group,
        ven_buyergroupid as buyer_group_id

    from source
)

select * from cleaned
