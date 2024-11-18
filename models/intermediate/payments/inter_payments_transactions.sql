-- models/intermediate/payments/int_payments_transactions.sql
with transactions as (
    select * from {{ ref('stg_coreruby_transactions') }}
),
credit_cards as (
    select * from {{ ref('stg_coreruby_credit_cards') }}
),
enriched_transactions as (
    select 
        t.id as transaction_id,
        t.price_cents,
        t.created_at,
        t.refunded_at,
        t.braintree_transaction_token,
        cc.card_type,
        cc.masked_number,
        t.purchase_id as appointment_id
    from transactions t
    left join credit_cards cc 
        on t.credit_card_id = cc.id
)
select * from enriched_transactions