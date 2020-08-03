INSERT INTO TMP.SPARK_KPIS_REG5

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    region_administrative,
    region_commerciale,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL

    --------- Revenue overview dont Voix
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont Voix' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_regionale,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,13)='REVENUE_VOICE' or SUBSTRING(DESTINATION_CODE,1,11)='REVENUE_SMS' or DESTINATION_CODE='UNKNOWN_BUN')
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL

    --------- Revenue overview dont  Equipements+OE
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont  Equipements+OE' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        null valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and 1=0
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ---------- Revenue overview dont sortant (~recharges)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont sortant (~recharges)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'RECHARGE' axe_regionale,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'VALEUR_AIRTIME'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Subscriber overview Subscriber base
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI ,
        null axe_revenue,
        'PARC 90 Jrs' axe_subscriber,
        null axe_regionale,
        source_table,
        'MAX'  cummulable,
        cast(sum(rated_amount) as bigint) valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



    UNION ALL
    ------- Subscriber overview Gross Adds
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI ,
        null axe_revenue,
        'GROSS ADDS' axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        cast(sum(rated_amount) as bigint) valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROSS_ADD'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Subscriber overview Churn
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI ,
        null axe_revenue,
        'CHURN' axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        cast(sum(rated_amount) as bigint) valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_CHURN'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ----TODO : les net add sont à traiter  et  corriger les donnnees regionales

    ------- Subscriber overview : Net adds
    select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI ,
        null axe_revenue,
        'NET ADDS' axe_subscriber,
        null axe_regionale,
        null source_table,
        'SUM' cummulable,
        sum(parcj7-parcj0) valeur,
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
    from (
        select region_id,cast(sum(rated_amount) as bigint) parcj0 from  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date= date_sub('###SLICE_VALUE###',6)   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )a
    left join  (
        select region_id,cast(sum(rated_amount) as bigint) parcj7 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date= '###SLICE_VALUE###'    and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )b on a.region_id=b.region_id
    left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
    group by
    c.administrative_region ,
    c.commercial_region


    UNION ALL
    ------- Subscriber Tx users (30jrs)
    -- TODO : corriger les donnnees regionales
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Tx users (30jrs)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'MOY' cummulable,
        cast(sum(rated_amount) as bigint) valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



    UNION ALL
    ------- Leviers de croissance : Revenue Data Mobile
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Revenue Data Mobile' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENU DATA' axe_regionale,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,12)='REVENUE_DATA' or DESTINATION_CODE='OM_DATA')
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Leviers de croissance : Price Per Megas

    select
         region_administrative,
         region_commerciale,
        'Leviers de croissance' category,
        'Price Per Megas' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'MOY' cummulable,
        max(valeur) valeur ,
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
        from (
            SELECT
                 a.region_administrative region_administrative,
                 a.region_commerciale region_commerciale,
                 cast(valeur_a/(valeur_b*1024) as double) valeur,
                 source_table
             from (
                SELECT
                 b.administrative_region region_administrative,
                 b.commercial_region region_commerciale,
                 cast(sum(rated_amount) as double ) valeur_a,
                 max(source_table) source_table
                 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
                left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
                where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,12)='REVENUE_DATA' or DESTINATION_CODE='OM_DATA')
                group by
                b.administrative_region ,
                b.commercial_region
            )a
            left join (
                select
                 b.administrative_region region_administrative,
                b.commercial_region region_commerciale,
                cast(sum(rated_amount) as double )  valeur_b
                from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
                left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
                where transaction_date ='###SLICE_VALUE###'   and KPI= 'USAGE'  and (SUBSTRING(DESTINATION_CODE,1,10)='USAGE_DATA')
                group by
                b.administrative_region ,
                b.commercial_region

            )b on a.region_administrative=b.region_administrative and a.region_commerciale=b.region_commerciale
        )a
        group by region_administrative,region_commerciale,source_table


    UNION ALL
    ------- Leviers de croissance : Users (30jrs, >1Mo)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users (30jrs, >1Mo)' KPI ,
        null axe_revenue,
        'DATA USERS (trafic >= 1Mo)' axe_subscriber,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    --TODO : corriger le parc par region
    UNION ALL
    ------- Leviers de croissance : Tx users data
    select
        a.region_administrative,
        a.region_commerciale,
        'Leviers de croissance' category,
        'Tx users data' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'MOY' cummulable,
        max(a.valeur/nvl(b.valeur,1)) valeur,
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
    FROM (
        select  b.administrative_region region_administrative,
                b.commercial_region region_commerciale,source_table,sum(rated_amount) valeur
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
        left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
        where transaction_date ='###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
        group by
        b.administrative_region ,
        b.commercial_region,
        source_table
    )a
    left join (
        SELECT b.administrative_region region_administrative,
                b.commercial_region region_commerciale,sum(rated_amount) valeur
        FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
        left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
        where transaction_date ='###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
        group by
        b.administrative_region ,
        b.commercial_region,
        source_table
    )b on a.region_administrative=b.region_administrative and a.region_commerciale=b.region_commerciale
    GROUP BY
        a.region_administrative,
        a.region_commerciale,
        source_table

    UNION ALL
       ------- Leviers de croissance : Revenue Orange Money
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- Leviers de croissance : Users (30jrs)
    select
         b.administrative_region region_administrative,
         b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users OM (30jrs)' KPI ,
        null axe_revenue,
        'Subs OM' axe_subscriber,
        null axe_regionale,
        null source_table,
        'MAX' cummulable,
        max(rated_amount)  valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI= 'PARC_OM_30Jrs'
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'CASH_IN_OM'
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI in( 'MERCH_PAY_OM','BILL_PAY_OM')
    group by
    b.administrative_region ,
    b.commercial_region


   UNION ALL
        ------- Distribution : Payments(Bill, Merch)
    select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Distribution' category,
        'Self Top UP ratio (%)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null, --source_table,
        'MOY' cummulable,
        sum(a.rated_amount/b.rated_amount) valeur,
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
    from (
        select region_id,sum(rated_amount) rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date ='###SLICE_VALUE###'   and KPI= 'REFILL_SELF_TOP' group by region_id
    ) a
    left join (
        select region_id, sum(rated_amount) rated_amount  from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date ='###SLICE_VALUE###'   and KPI= 'VALEUR_AIRTIME' group by region_id
     )  b on a.region_id=b.region_id
    left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
    group by
    c.administrative_region ,
    c.commercial_region

   UNION ALL
    ----TODO : partie Distribution à traiter (Recharges (C2S, CAG, OM))
    ------- Distribution : Recharges (C2S, CAG, OM)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Recharges (C2S, CAG, OM)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'VALEUR_AIRTIME'
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date =  '###SLICE_VALUE###'   and KPI= 'BALANCE_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



    UNION ALL
    ----TODO : partie Distribution à traiter (Nombre de Pos Airtime actif)
    ------- Distribution : Nombre de Pos Airtime actif
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Nombre de Pos Airtime actif' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'MAX' cummulable,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'POS_AIRTIME'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table




    UNION ALL
    ----TODO : partie Distribution à traiter (Nombre de Pos OM actif)
    ------- Distribution : Nombre de Pos OM actif
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Nombre de Pos OM actif' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'MAX' cummulable,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date   ='###SLICE_VALUE###' and KPI= 'PDV_OM_ACTIF_30Jrs'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



    UNION ALL
    select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Distribution' category,
        'Niveau de stock @ distributor level (nb jour)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null , --source_table,
        'MOY' cummulable,
        sum(a.rated_amount)/sum(b.rated_amount) valeur,
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
    from (
        select region_id,sum(rated_amount) rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date ='###SLICE_VALUE###'   and KPI= 'SNAPSHOT_STOCK_DIST' group by region_id
    ) a
    left join  (
            select region_id,sum(rated_amount)  rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date ='###SLICE_VALUE###'   and KPI= 'AVG_REFILL_DIST' group by region_id
    ) b on a.region_id=b.region_id
   left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
   group by
    c.administrative_region ,
    c.commercial_region


    UNION ALL
    ------- Distribution : Niveau de stock @ retailer level (nb jour)
    select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Distribution' category,
        'Niveau de stock @ retailer level (nb jour)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null , --source_table,
        'MOY' cummulable,
        sum(a.rated_amount)/sum(b.rated_amount) valeur,
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
    from (
        select region_id,sum(rated_amount) rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date ='###SLICE_VALUE###'   and KPI= 'SNAPSHOT_STOCK_CLIENT' group by region_id
    ) a
    left join  (
        select region_id,sum(rated_amount)  rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 where transaction_date ='###SLICE_VALUE###'   and KPI= 'AVG_REFILL_CLIENT' group by region_id
    ) b
   left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
   group by
    c.administrative_region ,
    c.commercial_region








    UNION ALL
    ----TODO : partie Digital à traiter (Engagements)
    ------- Digital : Engagements
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Digital' category,
        'Engagements' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        null valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ----TODO : partie CX à traiter (Pourcentage appel traité par BOT)
    ------- CX : Pourcentage appel traité par BOT
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Digital' category,
        'Pourcentage appel traité par BOT' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        null valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



    UNION ALL
    ----TODO : partie CX à traiter (Pourcentage traité en numérique)
    ------- CX : Pourcentage traité en numérique
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Digital' category,
        'Pourcentage traité en numérique' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        null valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ----TODO : partie CX à traiter (Durée moyenne de réponse numérique)
    ------- CX :Durée moyenne de réponse numérique
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Digital' category,
        'Durée moyenne de réponse numérique' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        source_table,
        'SUM' cummulable,
        null valeur,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V4 a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

)a
group by
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    cummulable