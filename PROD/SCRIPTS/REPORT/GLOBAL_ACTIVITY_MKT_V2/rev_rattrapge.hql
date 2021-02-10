 select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Payments(Bill, Merch)' KPI ,
        'Payments(Bill, Merch)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2021-02-03'   and KPI in( 'MERCH_PAY_OM','BILL_PAY_OM')
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

UNION ALL
select
      b.administrative_region region_administrative,
      b.commercial_region region_commerciale,
     'Leviers de croissance' category,
     'Cash In Valeur' KPI ,
     'Cash In Valeur' axe_vue_transversale ,
     null axe_revenu,
     null axe_subscriber,
     source_table source_table,
     'SUM' cummulable,
     sum(rated_amount)  valeur
 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
 left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
 where transaction_date ='2021-02-03'   and KPI= 'CASH_IN_OM'
 group by
 b.administrative_region ,
 b.commercial_region,
 source_table
 UNION ALL
 ------- Leviers de croissance : Cash In Valeur
 select
      b.administrative_region region_administrative,
      b.commercial_region region_commerciale,
     'Leviers de croissance' category,
     'Cash Out Valeur' KPI ,
     'Cash Out Valeur' axe_vue_transversale ,
     null axe_revenu,
     null axe_subscriber,
     source_table source_table,
     'SUM' cummulable,
     sum(rated_amount)  valeur
 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
 left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
 where transaction_date ='2021-02-03'   and KPI= 'CASH_OUT_OM'
 group by
 b.administrative_region ,
 b.commercial_region,
 source_table
 
 UNION ALL 
select
      b.administrative_region region_administrative,
      b.commercial_region region_commerciale,
     'Leviers de croissance' category,
     'Users OM (30jrs)' KPI ,
     'Users OM (30jrs)' axe_vue_transversale ,
     null axe_revenu,
     'Subs OM' axe_subscriber,
     source_table source_table,
     'MAX' cummulable,
     sum(rated_amount)  valeur
 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
 left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
 where transaction_date = '2021-02-03'   and KPI= 'PARC_OM_30Jrs'
 group by
 b.administrative_region ,
 b.commercial_region,
 source_table
 UNION ALL
select
     b.administrative_region region_administrative,
     b.commercial_region region_commerciale,
     'Leviers de croissance' category,
     'Revenue Orange Money' KPI ,
     'Revenue Orange Money' axe_vue_transversale ,
     'REVENU OM' axe_revenu,
     null axe_subscriber,
     source_table source_table,
     'SUM' cummulable,
     sum(rated_amount) valeur
 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
 left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
 where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE_OM'
 group by
 b.administrative_region ,
 b.commercial_region,
 source_table


select sum(rated_amount) rated_amount ,max(source_table) source_table from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date ='###SLICE_VALUE###'   and KPI in ('REFILL_SELF_TOP','DATA_VIA_OM')
