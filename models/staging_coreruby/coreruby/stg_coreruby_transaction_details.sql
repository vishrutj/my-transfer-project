-- models/staging_coreruby/coreruby/stg_coreruby_transaction_details.sql
with source as (
    select * except(name),
        upper(name) as name,
        row_number() over(
            partition by id 
            order by metadata.kafka_metadata.offset desc
        ) as row_num
    from {{ source('coreruby', 'transaction_details') }}
    where date(_partitiontime) is not null
    and kind = 'tax'
    and (UPPER(name) like '%GST%' 
         or UPPER(name) like '%HST%' 
         or UPPER(name) like '%PST%')
)
select * except(row_num)
from source
where row_num = 1