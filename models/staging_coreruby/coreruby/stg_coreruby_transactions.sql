-- models/staging/coreruby/stg_coreruby_transactions.sql
with source as (
    select * from {{ source('coreruby', 'transactions') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
        price_cents,
        created_at,
        refunded_at,
        braintree_transaction_token,
        credit_card_id,
        purchase_id,
        metadata,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1