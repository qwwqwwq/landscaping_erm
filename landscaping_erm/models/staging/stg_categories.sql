with source as (
    select * from {{ source('erm_data', 'dbo_CATEGORY') }}
),

cleaned as (
    select
        cat_id as category_id,
        trim(cat_categoryid) as category_code,
        trim(cat_categorydesc) as category_description,
        cat_taxid as tax_id,
        cat_cost as default_cost,
        cat_price1 as price_level_1,
        cat_price2 as price_level_2,
        cat_price3 as price_level_3,
        cat_price4 as price_level_4,
        cat_price5 as price_level_5,
        cat_price6 as price_level_6,
        cat_mu1 as markup_pct_1,
        cat_mu2 as markup_pct_2,
        cat_mu3 as markup_pct_3,
        cat_mu4 as markup_pct_4,
        cat_mu5 as markup_pct_5,
        cat_mu6 as markup_pct_6,
        trim(cat_gpricing) as group_pricing,
        trim(cat_forsku) as sku_format,
        trim(cat_roundopt) as rounding_option,
        cat_roundnmbr as rounding_number,
        cat_isasset as is_asset_flag,
        cat_pricegroupid as price_group_id,
        trim(cat_webdescription) as web_description

    from source
)

select * from cleaned
