INSERT INTO TMP.SPARK_KPIS_REG5

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    nvl(region_administrative,'INCONNU') region_administrative,
    nvl(region_commerciale,'INCONNU') region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    'DAILY' granularite,
    sum(valeur) valeur,
    cummulable,
    '###SLICE_VALUE###' processing_date
    from (

    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'Telco (prepayé+hybrid) + OM' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_regionale,
        source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur,
        null valeur_2wa,
        null valeur_3wa,
        null valeur_4wa,
        null valeur_mtd,
        null valeur_lmtd,
        null valeur_mtd_vs_lmdt,
        null valeur_mtd_last_year,
        null valeur_mtd_vs_budget,
        current_timestamp insert_date,
        current_date processing_date
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE_OM'
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
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
        'SUM' cummulable,
        sum(rated_amount)  valeur,
        null valeur_2wa,
        null valeur_3wa,
        null valeur_4wa,
        null valeur_mtd,
        null valeur_lmtd,
        null valeur_mtd_vs_lmdt,
        null valeur_mtd_last_year,
        null valeur_mtd_vs_budget,
        current_timestamp insert_date,
        current_date processing_date
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'CASH_IN_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table
    UNION ALL
    select
         b.administrative_region region_administrative,
         b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Cash Out Valeur' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
        'SUM' cummulable,
        sum(rated_amount)  valeur,
        null valeur_2wa,
        null valeur_3wa,
        null valeur_4wa,
        null valeur_mtd,
        null valeur_lmtd,
        null valeur_mtd_vs_lmdt,
        null valeur_mtd_last_year,
        null valeur_mtd_vs_budget,
        current_timestamp insert_date,
        current_date processing_date
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'CASH_OUT_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table
    UNION ALL
    ------- Leviers de croissance : Payments(Bill, Merch)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Payments(Bill, Merch)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur,
        null valeur_2wa,
        null valeur_3wa,
        null valeur_4wa,
        null valeur_mtd,
        null valeur_lmtd,
        null valeur_mtd_vs_lmdt,
        null valeur_mtd_last_year,
        null valeur_mtd_vs_budget,
        current_timestamp insert_date,
        current_date processing_date
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI in( 'MERCH_PAY_OM','BILL_PAY_OM')
    group by
    b.administrative_region ,
    b.commercial_region

    UNION ALL
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Revenue Orange Money' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENU OM' axe_regionale,
        null source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur,
        null valeur_2wa,
        null valeur_3wa,
        null valeur_4wa,
        null valeur_mtd,
        null valeur_lmtd,
        null valeur_mtd_vs_lmdt,
        null valeur_mtd_last_year,
        null valeur_mtd_vs_budget,
        current_timestamp insert_date,
        current_date processing_date
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table
    UNION ALL
    ----TODO : partie Distribution à traiter (Stock total client)
    ------- Distribution : Stock total client
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Stock total client(OM)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur,
        null valeur_2wa,
        null valeur_3wa,
        null valeur_4wa,
        null valeur_mtd,
        null valeur_lmtd,
        null valeur_mtd_vs_lmdt,
        null valeur_mtd_last_year,
        null valeur_mtd_vs_budget,
        current_timestamp insert_date,
        current_date processing_date
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date =  '###SLICE_VALUE###'   and KPI= 'BALANCE_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



)a
group by
    nvl(region_administrative,'INCONNU') ,
    nvl(region_commerciale,'INCONNU'),
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    cummulable