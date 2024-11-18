-- models/staging/coreruby/stg_coreruby_appointments.sql
with source as (
    select * from {{ source('coreruby', 'appointments') }}
    where date(_partitiontime) is not null
),
latest_records as (
    select 
        id,
        consultant_id,
        consumer_network_id,
        appointment_service_type_uuid,
        created_at,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from source
)
select * except(row_num)
from latest_records
where row_num = 1