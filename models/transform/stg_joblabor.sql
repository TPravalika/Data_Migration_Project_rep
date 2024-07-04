{{
    config(
        alias="JOBLABOR",
       
    )
}}
with cte as (


select
twt.WO_TASK_ORDER*10 as JPTASK,
twjl.CRAFT_CODE as CRAFT,
twjl.PLAN_LABOR_PLAN_CREW_SIZE as QUANTITY,
twjl.PLAN_LABOR_PLAN_HOURS as LABORHRS,
'AG_'||twj.TEMPLATE_WO_ID as JPNUM,
'NRPC' as ORGID,
'AMTRAK' as SITEID,
twjl.TMPLT_WO_JOB_LBR_ID as LABORCODE,
count(*) as duplicate_count


FROM
{{ source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{ source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{ source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID  LEFT JOIN
{{ source('spear','TA_TMPLT_WO_JOB_LBR')}} twjl on twj.TEMPLATE_WO_ID=twjl.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twjl.TEMPLATE_WO_TASK_ID and  twj.TEMPLATE_WO_JOB_ID=twjl.TEMPLATE_WO_JOB_ID 
where 
twjl.CRAFT_CODE IS NOT NULL and 
twjl.PLAN_LABOR_PLAN_CREW_SIZE IS NOT NULL and  
twjl.PLAN_LABOR_PLAN_HOURS IS NOT NULL 


GROUP BY (JPTASK,
CRAFT,
QUANTITY,
LABORHRS,
JPNUM,
ORGID,
SITEID,
LABORCODE)

)
select 
JPTASK,
CRAFT,
QUANTITY,
LABORHRS,
JPNUM,
ORGID,
SITEID,
LABORCODE
 from cte