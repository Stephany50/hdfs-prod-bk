  select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI ,
        'Net adds' axe_vue_transversale ,
        null axe_revenu,
        'NET ADDS' axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(parcj7-parcj0) valeur
    from (
        select region_id,cast(sum(rated_amount) as bigint) parcj0,max(source_table) source_table from  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date= date_sub('2020-12-20',6)   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )a
    left join  (
        select region_id,cast(sum(rated_amount) as bigint) parcj7 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date= '2020-12-20'    and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )b on a.region_id=b.region_id
    left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
    group by
    c.administrative_region ,
    c.commercial_region,
    source_table
