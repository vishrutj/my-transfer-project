-- models/staging_coreruby/coreruby/stg_coreruby_consumer_networks.sql
with source as (
    select * from {{ source('coreruby', 'consumer_networks') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
        name,
        region_id,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1