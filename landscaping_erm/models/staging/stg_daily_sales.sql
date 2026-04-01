with source as (
    select * from {{ source('erm_data', 'dbo_DAILY') }}
),

cleaned as (
    select
        {{ clarion_date('dai_date') }} as sales_date,
        dai_divisionid as division_id,
        dai_amount as gross_sales,
        dai_ramount as returns_amount,
        dai_subtotal as subtotal,
        dai_cost as cost_of_goods,
        dai_number as transaction_count,
        dai_ytd as year_to_date_sales,

        -- Cash flow breakdown
        dai_icash as invoice_cash,
        dai_iaccount as invoice_on_account,
        dai_oamount as order_amount,
        dai_ocash as order_cash,
        dai_mcash as misc_cash,
        dai_lamount as layaway_amount,
        dai_mamount as misc_amount,
        dai_lcash as layaway_cash,
        dai_samount as service_amount,
        dai_scash as service_cash,
        dai_rcash as return_cash,
        dai_raccount as return_on_account,
        dai_arcash as ar_cash,

        -- Tax
        dai_tax1 as tax_1,
        dai_tax2 as tax_2,
        dai_rtax1 as return_tax_1,
        dai_rtax2 as return_tax_2,

        -- Hourly sales amounts (hour 1 = midnight-1am, etc.)
        dai_h1 as hourly_sales_h01,
        dai_h2 as hourly_sales_h02,
        dai_h3 as hourly_sales_h03,
        dai_h4 as hourly_sales_h04,
        dai_h5 as hourly_sales_h05,
        dai_h6 as hourly_sales_h06,
        dai_h7 as hourly_sales_h07,
        dai_h8 as hourly_sales_h08,
        dai_h9 as hourly_sales_h09,
        dai_h10 as hourly_sales_h10,
        dai_h11 as hourly_sales_h11,
        dai_h12 as hourly_sales_h12,
        dai_h13 as hourly_sales_h13,
        dai_h14 as hourly_sales_h14,
        dai_h15 as hourly_sales_h15,
        dai_h16 as hourly_sales_h16,
        dai_h17 as hourly_sales_h17,
        dai_h18 as hourly_sales_h18,
        dai_h19 as hourly_sales_h19,
        dai_h20 as hourly_sales_h20,
        dai_h21 as hourly_sales_h21,
        dai_h22 as hourly_sales_h22,
        dai_h23 as hourly_sales_h23,
        dai_h24 as hourly_sales_h24,

        -- Hourly transaction counts
        dai_n1 as hourly_count_h01,
        dai_n2 as hourly_count_h02,
        dai_n3 as hourly_count_h03,
        dai_n4 as hourly_count_h04,
        dai_n5 as hourly_count_h05,
        dai_n6 as hourly_count_h06,
        dai_n7 as hourly_count_h07,
        dai_n8 as hourly_count_h08,
        dai_n9 as hourly_count_h09,
        dai_n10 as hourly_count_h10,
        dai_n11 as hourly_count_h11,
        dai_n12 as hourly_count_h12,
        dai_n13 as hourly_count_h13,
        dai_n14 as hourly_count_h14,
        dai_n15 as hourly_count_h15,
        dai_n16 as hourly_count_h16,
        dai_n17 as hourly_count_h17,
        dai_n18 as hourly_count_h18,
        dai_n19 as hourly_count_h19,
        dai_n20 as hourly_count_h20,
        dai_n21 as hourly_count_h21,
        dai_n22 as hourly_count_h22,
        dai_n23 as hourly_count_h23,
        dai_n24 as hourly_count_h24

    from source
)

select * from cleaned
