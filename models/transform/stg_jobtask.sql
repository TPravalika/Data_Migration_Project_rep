{{
    config(
        alias="JOBTASK",
       
    )
}}

with cte as (
select * from {{ ref ('stg_parent_jobplan_task')}} 
union 
select * from {{ ref ('stg_nested_jobplan_task')}} 
)
select * from cte
