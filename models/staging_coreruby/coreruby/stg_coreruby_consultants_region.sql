-- models/staging_coreruby/coreruby/stg_coreruby_consultants_regions.sql
with source as (
    select * from {{ source('coreruby', 'consultants_regions') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        consultant_id,
        region_id,
        specialism_id,
        row_number() over(
            partition by consultant_id, region_id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1