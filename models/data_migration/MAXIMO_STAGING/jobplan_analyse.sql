{{
    config(
        materialized="view",
        alias="jobplan_analyse",
        schema="SPEAR"
    )
}}

/*
select 
twt.TEMPLATE_WO_ID,
twt.TEMPLATE_WO_TASK_ID,
twt.WO_TASK_TITLE,
twt.WO_TASK_DURATION,
two.WO_PRIORITY_CODE,
twj.WO_JOB_DESC,
twj.TEMPLATE_WO_JOB_ID,
twj.WO_JOB_CODE,
twjl.TEMPLATE_WO_JOB_ID,
twjl.CRAFT_CODE,
twjl.PLAN_LABOR_PLAN_CREW_SIZE,
twjl.PLAN_LABOR_PLAN_HOURS,
twji.TEMPLATE_WO_JOB_ID,
twji.ITEM_ID,
twji.PLAN_ITEM_PLAN_QTY,
twjd.DOCUMENT_ID,
d.DOCUMENT_ID,
d.DOCUMENT_NAME,
d.DOCUMENT_NAME,
d.DOC_EXTERNAL_FILE,
d.DOC_EXTERNAL_FILE,
d.DOCUMENT_NAME,
d.DOC_EXTERNAL_FILE
*/
select 
 two.TEMPLATE_WO_ID ,
 twt.TEMPLATE_WO_TASK_ID,
 tet.WO_TASK_TITLE



from 
{{source('my_project','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('my_project','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{source('my_project','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID  LEFT JOIN
{{source('my_project','TA_TMPLT_WO_JOB_LBR')}} twjl on twj.TEMPLATE_WO_ID=twjl.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twjl.TEMPLATE_WO_TASK_ID and  twj.TEMPLATE_WO_JOB_ID=twjl.TEMPLATE_WO_JOB_ID LEFT JOIN
{{source('my_project','TA_TMPLT_WO_JOB_ITEM')}} twji on twji.TEMPLATE_WO_JOB_ID=twj.TEMPLATE_WO_JOB_ID and twji.TEMPLATE_WO_ID=twt.TEMPLATE_WO_ID and twji.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID LEFT JOIN
{{source('my_project','TA_TMPLT_WO_JOB_DOC')}} twjd on twjd.TEMPLATE_WO_JOB_ID=twj.TEMPLATE_WO_JOB_ID and twjd.TEMPLATE_WO_ID=twt.TEMPLATE_WO_ID and twjd.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID LEFT JOIN
{{source('my_project','TA_DOCUMENT')}} d on d.DOCUMENT_ID=twjd.DOCUMENT_ID LEFT JOIN
{{source('my_project','TA_DOC_CATEGORY')}} dc on d.DOC_CATEGORY_CODE=dc.DOC_CATEGORY_CODE


