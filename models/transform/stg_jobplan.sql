{{
    config(
        alias="JOBPLAN",
       
    )
}}

with cte as (
select * from {{ ref ('stg_parent_jobplan')}} 
union 
select * from {{ ref ('stg_nested_jobplan')}} 
)
select * from cte