-- models/staging_coreruby/coreruby/stg_coreruby_specialism_categories.sql
with source as (
    select * from {{ source('coreruby', 'specialism_categories') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
        new_name as category_name,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1