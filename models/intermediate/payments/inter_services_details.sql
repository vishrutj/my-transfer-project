-- models/intermediate/services/int_services_details.sql
with specialisms as (
    select * from {{ ref('stg_coreruby_specialisms') }}
),
specialism_categories as (
    select * from {{ ref('stg_coreruby_specialism_categories') }}
),
service_types as (
    select * from {{ ref('stg_coreruby_appointment_service_types') }}
),
enriched_services as (
    select
        s.id as specialism_id,
        s.specialism_name,
        sc.category_name as specialism_category,
        ast.uuid as service_type_uuid,
        ast.name as service_name,
        ast.grouping as service_group
    from specialisms s
    left join specialism_categories sc 
        on s.specialism_category_id = sc.id
    left join service_types ast 
        on s.id::string = ast.uuid
)
select * from enriched_services