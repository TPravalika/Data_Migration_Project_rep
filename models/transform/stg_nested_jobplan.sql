{{
    config(
        alias="NESTED_JOBPLAN",
       
    )
}}

with task_count_tab as (
    select two.TEMPLATE_WO_ID as TEMPLATE_WO_ID,count(distinct( TEMPLATE_WO_TASK_ID)) as task_count  
FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID 
group by two.TEMPLATE_WO_ID
 )


select 
'AG_'||twt.TEMPLATE_WO_TASK_ID as JPNUM,
twt.WO_TASK_TITLE as DESCRIPTION,
'NRPC' as ORGID,
'AMTRAK' as SITEID,
0 as PLUSCREVNUM,
'ACTIVE' as STATUS,
'MAINTENANCE' as TEMPLATETYPE,
twt.WO_TASK_EST_HRS as JPDURATION,
two.WO_PRIORITY_CODE as PRIORITY


FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{source('spear','TA_PATTERN_ELEMENT')}} pe on two.TEMPLATE_WO_ID=pe.TEMPLATE_WO_ID  LEFT JOIN 
{{source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID 
LEFT JOIN task_count_tab on two.TEMPLATE_WO_ID=task_count_tab.TEMPLATE_WO_ID
WHERE task_count_tab.task_count>1
