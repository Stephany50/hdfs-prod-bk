select * from (
    select  service_code, source_data,usage_description,sum(TAXED_AMOUNT) TAXED_AMOUNT
    from (
        select *  from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY  where transaction_date='2020-07-20' and TRAFFIC_MEAN='REVENUE'
    ) f
    left join dim.dt_usages  on service_code = usage_code
    where UPPER(USAGE_CODE) in ('NVX_PAR')
    group by service_code, source_data,usage_description
)t

