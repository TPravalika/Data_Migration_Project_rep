{{
    config(
        materialized="table",
        alias="DOCLINKS",
        database="MAXIMO_TRANSFORMED",
        schema="MAXIMO"
    )
}}

select 
td.DOCUMENT_ID,
td.DOCUMENT_NAME as DOCUMENT ,
td.DOCUMENT_NAME as DESCRIPTION ,
td.DOC_EXTERNAL_FILE as URLNAME,
'URL' as URLTYPE ,
'Attachments' AS DOCTYPE,
td.DOC_EXTERNAL_FILE as  DOCUMENTDATA

FROM
{{ source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{ source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{ source('spear','TA_TEMPLATE_WO_JOB')}} twj on twt.TEMPLATE_WO_ID=twj.TEMPLATE_WO_ID and twj.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID  LEFT JOIN
{{ source('spear','TA_TMPLT_WO_JOB_DOC')}}  twjd on twjd.TEMPLATE_WO_JOB_ID=twj.TEMPLATE_WO_JOB_ID and twjd.TEMPLATE_WO_ID=twt.TEMPLATE_WO_ID and twjd.TEMPLATE_WO_TASK_ID=twt.TEMPLATE_WO_TASK_ID LEFT JOIN
{{ source('spear','TA_DOCUMENT')}}  td  on td.DOCUMENT_ID=twjd.DOCUMENT_ID


