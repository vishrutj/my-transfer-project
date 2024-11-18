-- models/marts/finance/fct_payment_tax_details.sql
-- (Replaces m1_report functionality)
with payment_details as (
    select * from {{ ref('int_payments_transactions') }}
),
appointments as (
    select * from {{ ref('int_payments_appointments') }}
),
tax_details as (
    select * from {{ ref('int_payments_tax_details') }}
),
service_details as (
    select * from {{ ref('int_services_details') }}
),
final as (
    select
        'M' as R_ID,
        '1' as R_TYPE,
        '000' as WORKSTATION,
        pd.transaction_id as RECEIPT_NUMBER,
        1 as ITEM_NUMBER,
        COALESCE(sd.service_type_uuid, 
                 cast(sd.specialism_id as string), 
                 'PRODUCT_DELETED') as PRODUCT_CODE,
        COALESCE(sd.service_name, 
                 sd.specialism_name, 
                 'PRODUCT_DELETED') as PRODUCT_DESC,
        '' as GL_ACCOUNT,
        1 as QUANTITY,
        COALESCE(pd.price_cents, 0) as AMOUNT,
        case 
            when pd.refunded_at is not null then '-'
            else '+'
        end as SIGN,
        case when td.gst_amount > 0 
             then 'false' else 'true' 
        end as GST_EXEMPT_FLAG,
        td.gst_amount as GST_AMOUNT,
        case 
            when pd.refunded_at is not null then '-'
            else '+'
        end as GST_SIGN,
        case when td.pst_amount > 0 
             then 'false' else 'true' 
        end as PST_EXEMPT_FLAG,
        td.pst_amount as PST_AMOUNT,
        case 
            when pd.refunded_at is not null then '-'
            else '+'
        end as PST_SIGN,
        case when td.hst_amount > 0 
             then 'false' else 'true' 
        end as HST_EXEMPT_FLAG,
        td.hst_amount as HST_AMOUNT,
        case 
            when pd.refunded_at is not null then '-'
            else '+'
        end as HST_SIGN,
        COALESCE(sd.service_group, 
                 sd.specialism_category, 
                 'PRODUCT_DELETED') as PRODUCT_GROUP
    from payment_details pd
    left join appointments a 
        on pd.appointment_id = a.appointment_id
    left join tax_details td 
        on pd.transaction_id = td.transaction_id
    left join service_details sd 
        on a.appointment_service_type_uuid = sd.service_type_uuid
)
select * from final
order by RECEIPT_NUMBER DESC