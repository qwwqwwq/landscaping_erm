with source as (
    select * from {{ source('erm_data', 'dbo_INVDET') }}
),

cleaned as (
    select
        ind_invno as item_id,
        ind_invoiceno as invoice_number,
        ind_invoiceitem as line_number,
        trim(ind_description) as description,
        trim(ind_barcode) as barcode,
        ind_quantity as quantity,
        ind_price as unit_price,
        ind_itemcost as item_cost,
        ind_taxamount as tax_amount,
        ind_disc as discount_pct,
        ind_comm as commission_amount,
        ind_markup as markup_pct,
        ind_oprice as original_price,
        ind_rprice as retail_price,
        ind_couponamount as coupon_amount,
        ind_referenceprice as reference_price,
        ind_couponsavings as coupon_savings,
        ind_custid as customer_id,
        ind_tax1 as tax_id,
        trim(ind_sales1) as salesperson_1_code,
        trim(ind_sales2) as salesperson_2_code,
        upper(trim(ind_status)) as line_status,
        trim(ind_um) as unit_of_measure,
        ind_dquan as default_quantity,
        ind_department as department_id,
        ind_catid as category_id,
        ind_v_id as vendor_id,
        ind_held as quantity_held,
        ind_bord as quantity_backordered,
        ind_oquan as original_quantity,
        ind_quantitydelivered as quantity_delivered,
        ind_quantitystaged as quantity_staged,
        ind_specialquantity as special_quantity,
        ind_unitsper as units_per,
        trim(ind_deliverystatus) as delivery_status,
        trim(ind_returncode) as return_code,
        trim(ind_notation) as notation,
        trim(ind_linenote) as line_note,
        ind_selldivisionid as sell_division_id,
        {{ clarion_date('ind_date') }} as line_date,
        {{ clarion_time('ind_time') }} as line_time

    from source
)

select * from cleaned
