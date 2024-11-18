-- models/intermediate/payments/int_payments_tax_details.sql
with tax_details as (
    select * from {{ ref('stg_coreruby_transaction_details') }}
),
tax_amounts as (
    select
        transaction_id,
        sum(case when name like '%GST%' then amount_cents else 0 end) as gst_amount,
        sum(case when name like '%HST%' then amount_cents else 0 end) as hst_amount,
        sum(case when name like '%PST%' then amount_cents else 0 end) as pst_amount
    from tax_details
    group by transaction_id
)
select * from tax_amounts