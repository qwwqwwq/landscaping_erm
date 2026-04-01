with source as (
    select * from {{ source('erm_data', 'dbo_PINVOICE') }}
),

cleaned as (
    select
        pnv_invoiceno as invoice_number,
        case upper(trim(pnv_status))
            when 'I' then 'Invoiced'
            when 'R' then 'Return'
            when 'V' then 'Void'
            when 'X' then 'Cancelled'
            else upper(trim(pnv_status))
        end as invoice_status,
        trim(pnv_posted) as posted_status,
        pnv_total as invoice_total,
        pnv_cost as cost_amount,
        pnv_tax as tax_1,
        pnv_tax2 as tax_2,
        pnv_ntax as net_tax,
        {{ clarion_date('pnv_salesdate') }} as sales_date,
        {{ clarion_time('pnv_time') }} as sales_time,
        {{ clarion_date('pnv_dateinv') }} as date_invoiced,
        {{ clarion_date('pnv_processeddate') }} as processed_date,
        {{ clarion_time('pnv_processedtime') }} as processed_time,
        pnv_custid as customer_id,
        pnv_shipid as ship_to_id,
        pnv_ar_id as ar_id,
        trim(pnv_salesname) as salesperson_name,
        trim(pnv_salesid) as salesperson_code,
        trim(pnv_cashier) as cashier_code,
        pnv_stationno as station_number,
        pnv_sn_id as sales_number_id,
        pnv_reference as reference_number,
        pnv_taxid as tax_id,
        pnv_divisionid as division_id,
        pnv_trannumber as transaction_number,
        pnv_arpaid as ar_paid_flag,
        pnv_weborder as web_order_flag,
        pnv_backorderinvoiceno as backorder_invoice_number,
        pnv_fbpointsawarded as loyalty_points_awarded,
        pnv_fbpointsbalance as loyalty_points_balance

    from source
)

select * from cleaned
