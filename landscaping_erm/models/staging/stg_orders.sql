with source as (
    select * from {{ source('erm_data', 'dbo_ORDERS') }}
),

cleaned as (
    select
        ord_id as order_id,
        ord_invoiceno as invoice_number,
        upper(trim(ord_status)) as order_status,
        {{ clarion_date('ord_salesdate') }} as sales_date,
        {{ clarion_time('ord_time') }} as sales_time,
        {{ clarion_date('ord_datedue') }} as date_due,
        {{ clarion_date('ord_finaldate') }} as final_date,
        {{ clarion_time('ord_finaltime') }} as final_time,
        ord_total as order_total,
        ord_cash as cash_amount,
        ord_check as check_amount,
        ord_chargecard as charge_card_amount,
        ord_onaccount as on_account_amount,
        ord_deposit as deposit_amount,
        ord_credit as credit_amount,
        ord_coupons as coupon_amount,
        ord_ntax as net_tax,
        ord_tax1 as tax_1,
        ord_tax2 as tax_2,
        ord_round as rounding_amount,
        ord_custid as customer_id,
        ord_shipid as ship_to_id,
        trim(ord_name) as customer_name,
        trim(ord_salesid) as salesperson_code,
        trim(ord_s_id) as secondary_salesperson_code,
        ord_station as station_number,
        ord_sn_id as sales_number_id,
        trim(ord_purchaseordr) as purchase_order_number,
        trim(ord_sostat) as sales_order_status,
        trim(ord_shipvia) as ship_via,
        trim(ord_custerm) as customer_terms,
        trim(ord_jobnumber) as job_number,
        ord_divisionid as division_id,
        ord_taxid as tax_id,
        ord_weborder as web_order_flag,
        ord_bord as backorder_quantity,
        ord_totalcontractamount as total_contract_amount,
        trim(ord_dropstatus) as drop_ship_status,
        ord_credithold as credit_hold_flag,
        {{ clarion_date('ord_statusexpirationdate') }} as status_expiration_date,
        {{ clarion_date('ord_modifieddate') }} as modified_date,
        {{ clarion_time('ord_modifiedtime') }} as modified_time

    from source
)

select * from cleaned
