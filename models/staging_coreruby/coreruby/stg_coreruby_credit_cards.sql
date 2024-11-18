-- models/staging_coreruby/coreruby/stg_coreruby_credit_cards.sql
with source as (
    select * from {{ source('coreruby', 'credit_cards') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
        card_type,
        masked_number,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1