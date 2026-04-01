with source as (
    select * from {{ source('erm_data', 'dbo_AR_TRN') }}
),

cleaned as (
    select
        art_tnum as transaction_id,
        art_bnum as batch_number,
        case upper(trim(art_type))
            when 'CHG' then 'Charge'
            when 'PMT' then 'Payment'
            when 'CRD' then 'Credit'
            when 'FIN' then 'Finance Charge'
            when 'UNC' then 'Uncollectible'
            else upper(trim(art_type))
        end as transaction_type,
        art_custid as customer_id,
        art_invoiceno as invoice_number,
        trim(art_salesid) as salesperson_code,
        trim(art_glaccount) as gl_account,
        upper(trim(art_terms)) as payment_terms,
        trim(art_comt) as comment,
        art_ttot as transaction_total,
        art_paid as paid_amount,
        art_crdt as credit_amount,
        art_bala as balance,
        art_damt as discount_amount,
        art_dtak as discount_taken,
        art_dbal as discount_balance,
        art_pbal as prior_balance,
        art_dtot as detail_total,
        art_tpmt as total_payments,
        art_tdis as total_discounts,
        cast(art_tbat as numeric) as total_batch,
        cast(art_ttrn as numeric) as total_transactions,
        art_disputedamount as disputed_amount,
        art_calcdiscountamount as calculated_discount,
        case upper(trim(art_opcl))
            when 'P' then 'Paid'
            when 'O' then 'Open'
            when 'C' then 'Closed'
            else upper(trim(art_opcl))
        end as open_closed_status,
        art_posted as posted_flag,
        art_post as post_flag,
        art_statementid as statement_id,
        art_batch as batch_id,
        {{ clarion_date('art_tdte') }} as transaction_date,
        {{ clarion_date('art_dued') }} as due_date,
        {{ clarion_date('art_disd') }} as discount_date,
        {{ clarion_date('art_dateclosed') }} as date_closed

    from source
)

select * from cleaned
