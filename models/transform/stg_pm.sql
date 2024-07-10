{{
    config(
        alias="PM",
       
    )
}}

with cte as (
select * from {{ ref ('stg_parent_pm')}} 
union 
select * from {{ ref ('stg_child_pm')}} 
)
select * from cte