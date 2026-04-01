with source as (
    select * from {{ source('erm_data', 'dbo_ITEMS') }}
),

cleaned as (
    select
        ite_invno as item_id,
        trim(ite_description) as description,
        trim(ite_longdesc) as long_description,
        trim(ite_barcode) as barcode,
        trim(ite_catg) as category_name,
        ite_c_id as category_id,
        case upper(trim(ite_type))
            when 'I' then 'Inventory'
            when 'N' then 'Non-Inventory'
            when 'Z' then 'Inactive'
            when 'U' then 'Unknown'
            else upper(trim(ite_type))
        end as item_type,
        upper(trim(ite_sb)) as sell_by_unit,
        upper(trim(ite_pb)) as purchase_by_unit,
        ite_dquan as default_quantity,
        ite_comm as commission_rate,
        ite_scomm as sales_commission,
        ite_supp as supplier_cost,
        ite_weight as weight,
        ite_dept_id as department_id,
        trim(ite_deptname) as department_name,
        trim(ite_man_id) as manufacturer_id,
        ite_mf_id as manufacturer_fk_id,
        trim(ite_wcode) as warranty_code,
        trim(ite_subtype) as sub_type,
        trim(ite_freightclass) as freight_class,
        ite_depqnty as deposit_quantity,
        ite_askquan as ask_quantity_flag,
        ite_askprice as ask_price_flag,
        ite_foodstamps as food_stamps_eligible,
        ite_dropshipoptions as drop_ship_options,
        ite_daysuntilmature as days_until_mature,
        ite_daystohold as days_to_hold,
        ite_shareacrossdivisions as share_across_divisions_flag,
        ite_parentinvno as parent_item_id,
        trim(ite_replenishmentmethod) as replenishment_method

    from source
)

select * from cleaned
