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
from {{ source('my_project', 'TA_DOC_CATEGORY') }}
)
select *
from final