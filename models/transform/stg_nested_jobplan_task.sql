{{
    config(
        alias="NESTED_JOBPLAN_TASK",
       
    )
}}

with task_count_tab as (
    select two.TEMPLATE_WO_ID as TEMPLATE_WO_ID,count( TEMPLATE_WO_TASK_ID) as task_count  
FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID 
group by two.TEMPLATE_WO_ID
 )
select
twj.WO_JOB_ORDER*10 as JPTASK,
'AG_'||twt.TEMPLATE_WO_TASK_ID as JPNUM,
twj.WO_JOB_DESC as DESCRIPTION,
'NRPC' as ORGID,
'AMTRAK' as SITEID,
null as NESTEDJPNUM


FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
task_count_tab on task_count_tab.TEMPLATE_WO_ID =two.TEMPLATE_WO_ID LEFT JOIN 
{{source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID  

WHERE
 task_count_tab.task_count>1
