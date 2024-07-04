{{
    config(
        alias="JOBMATERIAL",
       
    )
}}
with cte as (

select
'NRPC' as ORGID,
'AMTRAK' as SITEID,
twt.WO_TASK_ORDER*10 as JPTASK,
'ITEMSET1' as ITEMSETID,
twji.ITEM_ID as ITEMNUM,
twji.PLAN_ITEM_PLAN_QTY as ITEMQTY,
'AG_'||twj.TEMPLATE_WO_ID as JPNUM,
count(*) as duplicate_count

FROM
{{ source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{ source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN
{{ source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID  LEFT JOIN
{{ source('spear','TA_TMPLT_WO_JOB_LBR')}} twjl on twj.TEMPLATE_WO_ID=twjl.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twjl.TEMPLATE_WO_TASK_ID and  twj.TEMPLATE_WO_JOB_ID=twjl.TEMPLATE_WO_JOB_ID LEFT JOIN
{{source('spear','TA_TMPLT_WO_JOB_ITEM')}} twji on twji.TEMPLATE_WO_JOB_ID=twj.TEMPLATE_WO_JOB_ID and twji.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twji.TEMPLATE_WO_TASK_ID=twj.TEMPLATE_WO_TASK_ID 
where 
twji.ITEM_ID is not null and 
twji.PLAN_ITEM_PLAN_QTY is not null

GROUP BY (
ORGID,
SITEID,
JPTASK,
ITEMSETID,
ITEMNUM,
ITEMQTY,
JPNUM
)
order by jpnum
)
select 
ORGID,
SITEID,
JPTASK,
ITEMSETID,
ITEMNUM,
ITEMQTY,
JPNUM
 from cte