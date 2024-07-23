{{
    config(
        materialized="table",
        alias="JP_DOCLINKS",
        database="MAXIMO_TRANSFORMED",
        schema="MAXIMO"
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
td.DOCUMENT_ID,
'JOBPLAN' as OWNERTABLE,
td.DOCUMENT_NAME as DOCUMENT ,
td.DOCUMENT_NAME as DESCRIPTION ,
td.DOC_EXTERNAL_FILE as URLNAME,
'URL' as URLTYPE ,
'Attachments' AS DOCTYPE,
td.DOC_EXTERNAL_FILE as  DOCUMENTDATA,

CASE 
WHEN task_count_tab.task_count>1 THEN 'AG_'||twjd.TEMPLATE_WO_TASK_ID
WHEN task_count_tab.task_count=1 THEN 'AG_'||twjd.TEMPLATE_WO_ID 
END 
as JPNUM ,
twj.WO_JOB_ORDER*10 
as JPTASK,
/*twjd.TEMPLATE_WO_ID,
twjd.TEMPLATE_WO_TASK_ID,
twt.WO_TASK_ORDER,
twjd.TEMPLATE_WO_JOB_ID,
twj.WO_JOB_ORDER*10
*/
FROM
{{ source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{ source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{ source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID  LEFT JOIN
{{ source('spear','TA_TMPLT_WO_JOB_DOC')}}  twjd on twjd.TEMPLATE_WO_JOB_ID=twj.TEMPLATE_WO_JOB_ID and twjd.TEMPLATE_WO_ID=twt.TEMPLATE_WO_ID and twjd.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID LEFT JOIN
{{ source('spear','TA_DOCUMENT')}}  td  on td.DOCUMENT_ID=twjd.DOCUMENT_ID left join 
task_count_tab on  two.TEMPLATE_WO_ID=task_count_tab.TEMPLATE_WO_ID

where td.document_id is not NULL 
--and jpnum='AG_152143'
order by twjd.TEMPLATE_WO_ID


