with source as (
    select * from {{ source('erm_data', 'dbo_ITEMSOLD') }}
),

cleaned as (
    select
        its_id as items_sold_id,
        its_invoiceno as invoice_number,
        case upper(trim(its_type))
            when 'I' then 'Invoice'
            when 'D' then 'Deposit'
            when 'A' then 'AR Payment'
            when 'C' then 'Cash Out'
            when 'V' then 'Void'
            when 'P' then 'Advance Pay'
            else upper(trim(its_type))
        end as transaction_type,
        trim(its_status) as transaction_status,
        trim(its_purpose) as transaction_purpose,
        {{ clarion_date('its_salesdate') }} as sales_date,
        {{ clarion_time('its_salestime') }} as sales_time,
        its_numbersold as number_sold,
        its_soldfor as sold_for_amount,
        its_cost as cost_amount,
        its_cash as cash_amount,
        its_check as check_amount,
        its_chargecard as charge_card_amount,
        its_debitcard as debit_card_amount,
        its_gift as gift_amount,
        its_onaccount as on_account_amount,
        its_financed as financed_amount,
        its_cashout as cash_out_amount,
        its_coupon as coupon_amount,
        its_checkrefund as check_refund_amount,
        its_roundamount as rounding_amount,
        its_stationno as station_number,
        trim(its_s_id) as salesperson_code,
        its_custid as customer_id,
        its_batch as batch_id,
        its_posted as posted_flag,
        trim(its_cc1) as cost_center_1,
        trim(its_cc2) as cost_center_2,
        trim(its_cc3) as cost_center_3

    from source
)

select * from cleaned
