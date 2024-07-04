{{
    config(
        materialized="table",
        alias="test1",
        database="MAXIMO_TRANSFORMED",
        schema="MAXIMO"
    )
}}

select * from dual