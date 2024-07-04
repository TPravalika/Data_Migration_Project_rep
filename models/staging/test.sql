{{
    config(
        materialized="view",
        alias="test",
        schema="SPEAR"
    )
}}
with final as 
(
    select * 
from {{ source('spear', 'TA_DOC_CATEGORY') }}
)
select *
from final