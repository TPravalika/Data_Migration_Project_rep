
{{
    config(
        materialized="table",
        alias="PARENT_PM",
        database="MAXIMO_TRANSFORMED",
        schema="MAXIMO"
    )
}}

with two_count_tab as (
select pm.MAINT_PATTERN_CODE,e.EQUIP_CODE,count(distinct(two.TEMPLATE_WO_ID)) as two_count
from
     {{ source('spear','TA_WO_INFORMATION')}}  woi 
    JOIN {{ source('spear','TA_EQUIPMENT')}}  e               ON e.EQUIP_ID = woi.EQUIP_ID                          
    JOIN {{ source('spear','TA_PATTERN_ELEMENT')}} pe        ON woi.PATTERN_ELEM_ID=pe.PATTERN_ELEM_ID and woi.MAINT_PATTERN_CODE=pe.MAINT_PATTERN_CODE
    JOIN {{ source('spear','TA_MAINT_PATTERN')}}  pm          ON pe.MAINT_PATTERN_CODE=pm.MAINT_PATTERN_CODE
    LEFT JOIN {{ source('spear','TA_TEMPLATE_WO')}}  two      ON woi.TEMPLATE_WO_ID=two.TEMPLATE_WO_ID
   
    group by pm.MAINT_PATTERN_CODE,e.EQUIP_CODE
    order by pm.MAINT_PATTERN_CODE,e.EQUIP_CODE
 )


Select 
trim(pm.MAINT_PATTERN_CODE)||'-'||trim(e.EQUIP_CODE) as PMNUM,
pm.MAINT_PATTERN_DESC as DESCRIPTION,
'NRPC' as ORGID,
'AMTRAK' as SITEID,
'ACTIVE' as STATUS,

e.EQUIP_CODE as LOCATION,
1 as LEADTIMEACTIVE,
two.WO_TYPE_CODE as WORKTYPE,
'REVIEWED' as WOSTATUS,
CASE
    WHEN  two_count_tab.two_count=1 THEN 'AG_'||two.TEMPLATE_WO_ID
    WHEN  two_count_tab.two_count>1 THEN null
    END as JPNUM,
'ME' as OWNERGROUP,
'AMTRAK' as STORELOCSITE,
pe.RECUR_INTERVAL_NBR as FREQUENCY,
CASE
    WHEN pe.CALENDAR_INTERVAL_CODE='D' THEN 'DAYS'
    WHEN pe.CALENDAR_INTERVAL_CODE='W' THEN 'WEEKS'
    END 
as FREQUNIT,
CASE
    WHEN  two_count_tab.two_count>1 THEN 'SEQUENTIAL_PATTERN'
    WHEN  two_count_tab.two_count=1 THEN null
    END  as BRDAPMSEQTYPE,

    CASE
    WHEN  two_count_tab.two_count>1 THEN 1
    WHEN  two_count_tab.two_count=1 THEN null
    END  as BRDAHIERSEQINUSE,
    null as WOSEQUENCE,
    null as PARENT
       
from 
         {{ source('spear','TA_WO_INFORMATION')}}  woi 
    JOIN {{ source('spear','TA_EQUIPMENT')}}  e               ON e.EQUIP_ID = woi.EQUIP_ID                          
    JOIN {{ source('spear','TA_PATTERN_ELEMENT')}} pe        ON woi.PATTERN_ELEM_ID=pe.PATTERN_ELEM_ID and woi.MAINT_PATTERN_CODE=pe.MAINT_PATTERN_CODE
    JOIN {{ source('spear','TA_MAINT_PATTERN')}}  pm          ON pe.MAINT_PATTERN_CODE=pm.MAINT_PATTERN_CODE
    LEFT JOIN {{ source('spear','TA_TEMPLATE_WO')}}  two      ON woi.TEMPLATE_WO_ID=two.TEMPLATE_WO_ID
   LEFT JOIN two_count_tab          ON pm.MAINT_PATTERN_CODE=two_count_tab.MAINT_PATTERN_CODE and e.EQUIP_CODE=two_count_tab.EQUIP_CODE
 
