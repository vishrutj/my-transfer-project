-- models/staging_coreruby/coreruby/stg_coreruby_appointment_service_types.sql
with source as (
    select * from {{ source('coreruby', 'appointment_service_types') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        uuid,
        name,
        grouping,
        row_number() over(
            partition by uuid 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1