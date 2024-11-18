-- models/intermediate/payments/int_payments_appointments.sql
with appointments as (
    select * from {{ ref('stg_coreruby_appointments') }}
),
consumer_networks as (
    select * from {{ ref('stg_coreruby_consumer_networks') }}
),
enriched_appointments as (
    select 
        a.id as appointment_id,
        a.consultant_id,
        cn.id as network_id,
        cn.name as province,
        a.appointment_service_type_uuid,
        a.created_at
    from appointments a
    left join consumer_networks cn 
        on a.consumer_network_id = cn.id
)
select * from enriched_appointments