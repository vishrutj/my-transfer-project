-- models/marts/finance/fct_payment_transactions.sql
-- (Replaces m0_report functionality)
with payment_transactions as (
    select * from {{ ref('int_payments_transactions') }}
),
payment_appointments as (
    select * from {{ ref('int_payments_appointments') }}
),
final as (
    select
        'M' as R_ID,
        '0' as R_TYPE,
        pa.province,
        pt.transaction_id as RECEIPT_NUMBER,
        pt.price_cents as PAYMENT_AMOUNT,
        case 
            when pt.refunded_at is not null then '-'
            else '+'
        end as SIGN,
        pt.braintree_transaction_token as AUTHORISATION_NUMBER,
        pt.card_type as PAYMENT_METHOD,
        FORMAT_DATE('%Y-%m-%d', pt.created_at) as DATE_OF_SALE,
        FORMAT_DATE('%H:%M:%S', pt.created_at) as TIME_OF_SALE,
        RIGHT(pt.masked_number, 4) as CREDIT_CARD_L4,
        LEFT(pt.masked_number, 6) as CREDIT_CARD_F6
    from payment_transactions pt
    left join payment_appointments pa 
        on pt.appointment_id = pa.appointment_id
)
select * from final
order by RECEIPT_NUMBER DESC