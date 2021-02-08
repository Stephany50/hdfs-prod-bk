select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Revenue Data Mobile' KPI ,
        'Revenue Data Mobile' axe_vue_transversale ,
       'REVENU DATA' axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum(case when source_table ='FT_SUBS_RETAIL_ZEBRA' then rated_amount*30/100 else rated_amount end) valeur
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2021-01-01'   and KPI= 'REVENUE' AND sub_account='MAIN' and (DESTINATION_CODE='REVENUE_DATA_BUNDLE' or DESTINATION_CODE='OM_DATA' or (DESTINATION_CODE='REVENUE_DATA_PAYGO' and service_code<>'NVX_GPRS_SVA') or source_table in ('FT_EMERGENCY_DATA','FT_DATA_TRANSFER','FT_SUBS_RETAIL_ZEBRA'))
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table
