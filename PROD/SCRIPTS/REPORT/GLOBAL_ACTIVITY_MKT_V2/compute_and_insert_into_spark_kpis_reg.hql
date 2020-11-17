INSERT INTO tmp.SPARK_KPIS_REG2

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    null region_administrative,
    null region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    'WEEKLY' granularite,
    sum(valeur) valeur,
    '###SLICE_VALUE###' processing_date
    from (
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Revenue overview' category,
        'Telco (prepayé+hybrid) + OM' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN'
    --group by
   -- b.administrative_region ,
   -- b.commercial_region,
    --source_table

    UNION ALL

    --------- Revenue overview dont Voix
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont Voix' KPI ,
        null axe_revenue,
        'REVENU VOIX SORTANT' axe_subscriber,
        null axe_regionale,
        null source_table,
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
   -- left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,13)='REVENUE_VOICE' or SUBSTRING(DESTINATION_CODE,1,11)='REVENUE_SMS' or DESTINATION_CODE='UNKNOWN_BUN')
    --group by
   -- b.administrative_region ,
   --b.commercial_region,
   -- source_table


    UNION ALL

    --------- Revenue overview dont  Equipements+OE
    select * from (select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont  Equipements+OE' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,13)='REVENUE_VOICE' or SUBSTRING(DESTINATION_CODE,1,11)='REVENUE_SMS')
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t


    UNION ALL
    ---------- Revenue overview dont sortant (~recharges)
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont sortant (~recharges)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'RECHARGE' axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'VALEUR_AIRTIME'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    UNION ALL
    ------- Subscriber overview Subscriber base
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI ,
        null axe_revenue,
        'PARC 90 Jrs' axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table



    UNION ALL
    ------- Subscriber overview Gross Adds
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI ,
        null axe_revenue,
        'GROSS ADDS' axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date  between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'  and KPI='PARC' and DESTINATION_CODE = 'USER_GROSS_ADD'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    UNION ALL
    ------- Subscriber overview Churn
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI ,
        null axe_revenue,
        'CHURN' axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'  and KPI='PARC' and DESTINATION_CODE = 'USER_CHURN'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    UNION ALL
    ----TODO : les net add sont à traiter PARCJ1=PARCJ0 +NETADDJ1 avec NETADDJ1=GROSSADSJ1-CHURNJ1
    ----- NETADDJ1 =PARCJ0 -PARCJ1 + GROSSADSJ1-CHURNJ1
    ------- Subscriber overview : Net adds
    select
        null, --b.administrative_region region_administrative,
        null , --b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI ,
        null axe_revenue,
        'NET ADDS' axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from (select cast(sum(rated_amount) as bigint) parcj0 from  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date= date_sub('###SLICE_VALUE###',6)   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' )a,
     (select cast(sum(rated_amount) as bigint) parcj7 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date= '###SLICE_VALUE###'    and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP')b
  --  left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
  --  where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_CHURN'
   -- group by
   -- b.administrative_region ,
    --b.commercial_region,
    --


    UNION ALL
    ------- Subscriber Tx users (30jrs)
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Tx users (30jrs)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table



    UNION ALL
    ------- Leviers de croissance : Revenue Data Mobile
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Revenue Data Mobile' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENU DATA' axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,12)='REVENUE_DATA' or DESTINATION_CODE='OM_DATA')
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    UNION ALL
    ------- Leviers de croissance : Price Per Megas

    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Price Per Megas' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
                 --a.region_administrative region_administrative,
                 --a.region_commerciale region_commerciale,
                 cast(valeur_a/(valeur_b*1024) as double) valeur
                 --source_table
             from (
                SELECT
                 --b.administrative_region region_administrative,
                 --b.commercial_region region_commerciale,
                 cast(sum(rated_amount) as double ) valeur_a
                 --max(source_table) source_table
                 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
                --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
                where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,12)='REVENUE_DATA' or DESTINATION_CODE='OM_DATA')
                --group by
                --b.administrative_region ,
                --b.commercial_region
            )a, (
                select
                 --b.administrative_region region_administrative,
                --b.commercial_region region_commerciale,
                cast(sum(rated_amount) as double )  valeur_b
                from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
                --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
                where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'USAGE'  and (SUBSTRING(DESTINATION_CODE,1,10)='USAGE_DATA')
                --group by
                --b.administrative_region ,
                --b.commercial_region

            )b --on a.region_administrative=b.region_administrative and a.region_commerciale=b.region_commerciale
        )a
        --group by region_administrative,region_commerciale,source_table


    UNION ALL
    ------- Leviers de croissance : Users (30jrs, >1Mo)
    select
        null region_administrative,-- b.administrative_region region_administrative,
        null region_commerciale ,--b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users (30jrs, >1Mo)' KPI ,
        null axe_revenue,
        'DATA USERS (trafic >= 1Mo)' axe_subscriber,
        null axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    --TODO : corriger le parc par region
    UNION ALL
    ------- Leviers de croissance : Tx users data
        select
            null region_administrative,-- a.administrative_region region_administrative,
            null region_commerciale ,--a.commercial_region region_commerciale,
            'Leviers de croissance' category,
            'Tx users data' KPI ,
            null axe_revenue,
            null axe_subscriber,
            null axe_regionale,
            null source_table,
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
            select
                --b.administrative_region region_administrative,
                  --  b.commercial_region region_commerciale,source_table,
                  sum(rated_amount) valeur
            from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
            --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
            where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
           -- group by
            --b.administrative_region ,
            --b.commercial_region,
            --source_table
        )a,(
            SELECT
                    --b.administrative_region region_administrative,
                    --b.commercial_region region_commerciale
                    sum(rated_amount) valeur
            FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
            --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
            where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
            --group by
            --b.administrative_region ,
            --b.commercial_region,
            --source_table
        )b --on a.region_administrative=b.region_administrative and a.region_commerciale=b.region_commerciale
        --GROUP BY
         --   a.region_administrative,
         --   a.region_commerciale,
          --  source_table

    UNION ALL
    ------- Leviers de croissance : Revenue Orange Money
    select
       null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Revenue Orange Money' KPI ,
        null axe_revenue,
        null axe_subscriber,
        'REVENU OM' axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REVENUE_OM'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table

    UNION ALL
    ------- Leviers de croissance : Users (30jrs)
    select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users (30jrs)' KPI ,
        null axe_revenue,
        'Subs OM' axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and KPI= 'PARC_OM_30Jrs'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    UNION ALL
    ------- Leviers de croissance : Cash In Valeur
    select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Cash In Valeur' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'CASH_IN_OM'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table

    UNION ALL
    ------- Leviers de croissance : Payments(Bill, Merch)
    select * from (select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Payments(Bill, Merch)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI in( 'MERCH_PAY_OM','BILL_PAY_OM')
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1
    )t



   UNION ALL
    ------- Distribution : Payments(Bill, Merch)
    select
        null, --b.administrative_region region_administrative,
        null, -- b.commercial_region region_commerciale,
        'Distribution' category,
        'Self Top UP ratio (%)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null, --source_table,
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
    from (select sum(rated_amount) rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'REFILL_SELF_TOP') a,
     (select sum(rated_amount) rated_amount  from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'VALEUR_AIRTIME')  b
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id

    --group by
   -- b.administrative_region ,
   -- b.commercial_region,
   -- source_table



   UNION ALL
    ----TODO : partie Distribution à traiter (Recharges (C2S, CAG, OM))
    ------- Distribution : Recharges (C2S, CAG, OM)
    select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Distribution' category,
        'Recharges (C2S, CAG, OM)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'VALEUR_AIRTIME'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table


    UNION ALL
    ----TODO : partie Distribution à traiter (Stock total client TELCO)
    ------- Distribution : Stock total client
    select * from (select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Distribution' category,
        'Stock total client(TELCO)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'BALANCE_OM'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t


    UNION ALL
    ----TODO : partie Distribution à traiter (Stock total client)
    ------- Distribution : Stock total client
    select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Distribution' category,
        'Stock total client(OM)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date =  '###SLICE_VALUE###'   and KPI= 'BALANCE_OM'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table



    UNION ALL
    ------- Distribution : Nombre de Pos Airtime actif
    select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Distribution' category,
        'Nombre de Pos Airtime actif' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'POS_AIRTIME'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table




    UNION ALL
    ------- Distribution : Nombre de Pos OM actif
    select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Distribution' category,
        'Nombre de Pos OM actif' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '###SLICE_VALUE###'   and destination_code= 'PDV_OM_ACTIF_30Jrs'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table





    UNION ALL
    ------- Distribution : Niveau de stock @ distributor level (nb jour)
    select
        null,-- b.administrative_region region_administrative,
        null, --b.commercial_region region_commerciale,
        'Distribution' category,
        'Niveau de stock @ distributor level (nb jour)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null , --source_table,
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
    from (select sum(rated_amount) rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'SNAPSHOT_STOCK_DIST') a,
     (select sum(rated_amount)  rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'AVG_REFILL_DIST') b
   -- left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id

  --  group by
   -- b.administrative_region ,
   -- b.commercial_region,
   -- source_table



    UNION ALL
    ------- Distribution : Niveau de stock @ retailer level (nb jour)
    select
        null,-- b.administrative_region region_administrative,
        null, --b.commercial_region region_commerciale,
        'Distribution' category,
        'Niveau de stock @ retailer level (nb jour)' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null , --source_table,
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
    from (select sum(rated_amount) rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'SNAPSHOT_STOCK_CLIENT') a,
     (select sum(rated_amount)  rated_amount from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'AVG_REFILL_CLIENT') b
   -- left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
--    group by
   -- b.administrative_region ,
  --  b.commercial_region,
   -- source_table





    UNION ALL
    ----TODO : partie Digital à traiter (My Orange users)
    ------- Digital : My Orange users
    select * from (select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Digital' category,
        'My Orange users' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t



    UNION ALL
    ----TODO : partie Digital à traiter (Engagements)
    ------- Digital : Engagements
    select * from (select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Digital' category,
        'Engagements' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t


    UNION ALL
    ----TODO : partie CX à traiter (Pourcentage appel traité par BOT)
    ------- CX : Pourcentage appel traité par BOT
    select * from (select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Digital' category,
        'Pourcentage appel traité par BOT' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    --group by
   -- b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t



    UNION ALL
    ----TODO : partie CX à traiter (Pourcentage traité en numérique)
    ------- CX : Pourcentage traité en numérique
    select * from (select
       null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Digital' category,
        'Pourcentage traité en numérique' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
    --group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t


    UNION ALL
    ----TODO : partie CX à traiter (Durée moyenne de réponse numérique)
    ------- CX :Durée moyenne de réponse numérique
    select * from (select
        null region_administrative,-- a.administrative_region region_administrative,
        null region_commerciale ,--a.commercial_region region_commerciale,
        'Digital' category,
        'Durée moyenne de réponse numérique' KPI ,
        null axe_revenue,
        null axe_subscriber,
        null axe_regionale,
        null source_table,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2 a
    --left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'   and KPI= 'UNIQUE_DATA_USERS'
   -- group by
    --b.administrative_region ,
    --b.commercial_region,
    --source_table
    limit 1)t

)a
group by
    --region_administrative,
    --region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale