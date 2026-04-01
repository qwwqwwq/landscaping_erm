with source as (
    select * from {{ source('erm_data', 'dbo_RETURNS') }}
),

cleaned as (
    select
        rtn_invoiceno as invoice_number,
        rtn_invoiceitem as invoice_item_number,
        trim(rtn_salesid) as salesperson_code,
        trim(rtn_authid) as authorized_by_code,
        trim(rtn_reason) as return_reason,
        {{ clarion_date('rtn_date') }} as return_date,
        {{ clarion_time('rtn_time') }} as return_time,
        rtn_invno as item_id,
        rtn_stk_id as stock_id,
        rtn_quan as quantity,
        trim(rtn_snum) as serial_number,
        trim(rtn_barcode) as barcode,
        trim(rtn_desc) as description,
        rtn_cost as cost,
        rtn_vi_id as vendor_invoice_id

    from source
)

select * from cleaned
