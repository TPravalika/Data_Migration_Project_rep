{{
    config(
        materialized="table",
        alias="PARENT_JOBPLAN",
        database="MAXIMO_TRANSFORMED",
        schema="MAXIMO"
    )
}}

select 
'AG_'||twt.TEMPLATE_WO_ID as JPNUM,
pe.PATTERN_ELEM_DESC as DESCRIPTION,
'NRPC' as ORGID,
'AMTRAK' as SITEID,
0 as PLUSCREVNUM,
'ACTIVE' as STATUS,
'MAINTENANCE' as TEMPLATETYPE,
twt.WO_TASK_DURATION as JPDURATION,
two.WO_PRIORITY_CODE as PRIORITY



FROM
{{source('spear','TA_TEMPLATE_WO_TASK')}} twt JOIN
{{source('spear','TA_TEMPLATE_WO')}} two on two.TEMPLATE_WO_ID= twt.TEMPLATE_WO_ID LEFT JOIN 
{{source('spear','TA_PATTERN_ELEMENT')}} pe on two.TEMPLATE_WO_ID=pe.TEMPLATE_WO_ID
