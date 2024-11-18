-- models/staging_coreruby/coreruby/stg_coreruby_consultants.sql
with source as (
    select * from {{ source('coreruby', 'consultants') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
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