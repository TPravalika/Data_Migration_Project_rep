
with task_count_tab as (
    select two.TEMPLATE_WO_ID as TEMPLATE_WO_ID,count( TEMPLATE_WO_TASK_ID) as task_count  
FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID 
group by two.TEMPLATE_WO_ID
 )
select
CASE
    WHEN task_count_tab.task_count=1 THEN twj.WO_JOB_ORDER*10
    ELSE twt.WO_TASK_ORDER*10 
    END  AS JPTASK,  
    'AG_'||twt.TEMPLATE_WO_ID as JPNUM,
CASE 
    WHEN task_count_tab.task_count=1 THEN twj.WO_JOB_DESC
    ELSE twt.WO_TASK_TITLE
    END AS DESCRIPTION,
'NRPC' as ORGID,
'AMTRAK' AS SITEID,
CASE 
    WHEN task_count_tab.task_count=1 THEN null
    ELSE 'AG_'||twt.TEMPLATE_WO_TASK_ID
    END AS NESTEDJPNUM,
0 AS TASKDURATION,
0 AS PLUSCJPREVNUM

FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID LEFT JOIN 
task_count_tab on task_count_tab.TEMPLATE_WO_ID =two.TEMPLATE_WO_ID 
  

  --remove for test purpose
  WHERE
  JPNUM='AG_212048'