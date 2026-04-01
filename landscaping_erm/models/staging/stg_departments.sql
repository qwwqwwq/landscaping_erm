with source as (
    select * from {{ source('erm_data', 'dbo_DEPTMENT') }}
),

cleaned as (
    select
        dpt_id as department_id,
        trim(dpt_code) as department_code,
        trim(dpt_description) as department_name,
        trim(dpt_autosku) as auto_sku_prefix,
        dpt_item as item_count,
        trim(dpt_toplevelgrouping) as top_level_grouping,
        trim(dpt_webdescription) as web_description

    from source
)

select * from cleaned
