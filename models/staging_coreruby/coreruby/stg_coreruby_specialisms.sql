-- models/staging_coreruby/coreruby/stg_coreruby_specialisms.sql
with source as (
    select * from {{ source('coreruby', 'specialisms') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
        specialism_category_id,
        new_name as specialism_name,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1