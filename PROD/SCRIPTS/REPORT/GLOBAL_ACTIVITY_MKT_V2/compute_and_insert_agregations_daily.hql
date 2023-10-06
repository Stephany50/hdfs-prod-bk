INSERT INTO AGG.SPARK_KPIS_DG_TMP

SELECT
    nvl(region_administrative,'INCONNU') region_administrative,
    nvl(region_commerciale,'INCONNU') region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    'DAILY' granularite,
    sum(valeur) valeur,
    cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    '2023-09-24' processing_date,
    max(SOURCE_TABLE) source_table
    from (
    -- GOS SVA , modif FnF ,rachat de validité , sos credit fees , trafic crbt , orange célébrité , Orange signature.
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'Telco (prepayé+hybrid) + OM' KPI ,
        'Telco (prepayé+hybrid) + OM' axe_vue_transversale ,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum (
        CASE
            WHEN TRANSACTION_DATE LIKE '%-01-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.581
            WHEN TRANSACTION_DATE LIKE '%-02-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.544
            WHEN TRANSACTION_DATE LIKE '%-03-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.581
            WHEN TRANSACTION_DATE LIKE '%-04-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.537
            WHEN TRANSACTION_DATE LIKE '%-05-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.540
            WHEN TRANSACTION_DATE LIKE '%-06-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.534
            WHEN TRANSACTION_DATE LIKE '%-07-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.578
            WHEN TRANSACTION_DATE LIKE '%-08-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.574
            WHEN TRANSACTION_DATE LIKE '%-09-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.573
            WHEN TRANSACTION_DATE LIKE '%-10-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.572
            WHEN TRANSACTION_DATE LIKE '%-11-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.571
            WHEN TRANSACTION_DATE LIKE '%-12-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.569
            ELSE rated_amount
        END ) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24' and KPI= 'REVENUE' AND sub_account='MAIN' --AND SOURCE_TABLE IN ('FT_A_SUBSCRIPTION','FT_GSM_TRAFFIC_REVENUE_DAILY','FT_A_GPRS_ACTIVITY','FT_OVERDRAFT','IT_ZTE_ADJUSTMENT','FT_SUBS_RETAIL_ZEBRA','FT_CREDIT_TRANSFER','FT_CONTRACT_SNAPSHOT')
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL

    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'Telco (prepayé+hybrid) + OM' KPI ,
        'Telco (prepayé+hybrid) + OM' axe_vue_transversale ,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'REVENUE_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL


    --------- Revenue overview dont Voix
    --TODO : faire la mise à jour du ref des subs et aligner le dwh et le dlk
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Revenue overview' category,
        'dont Voix' KPI ,
        'dont Voix' axe_vue_transversale ,
        'REVENU VOIX SORTANT' axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum(case when source_table ='FT_SUBS_RETAIL_ZEBRA' then rated_amount*70/100 else rated_amount end) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,13)='REVENUE_VOICE' or SUBSTRING(DESTINATION_CODE,1,11)='REVENUE_SMS' or source_table in('FT_SUBS_RETAIL_ZEBRA','FT_CREDIT_TRANSFER','FT_CONTRACT_SNAPSHOT') or  DESTINATION_CODE  in ('UNKNOWN_BUN'))
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
        'dont  Equipements+OE' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        null valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and 1=0
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
        'dont sortant (~recharges)' axe_vue_transversale ,
        'RECHARGE' axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'VALEUR_AIRTIME'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Subscriber overview Subscriber base
    --DONE :corriger les partitions des tables suivantes qui ne sont pas régionalisées(location_ci):  FT_GROUP_SU...,FT_COMMER...,FT_A_SUBS
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI ,
        'Subscriber base' axe_vue_transversale ,
         null axe_revenu,
        'PARC 90 Jrs' axe_subscriber,
        source_table,
        'MAX'  cummulable,
        cast(sum(rated_amount) as bigint) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
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
        'Gross Adds' axe_vue_transversale ,
        null axe_revenu,
        'GROSS ADDS' axe_subscriber,
        source_table,
        'SUM' cummulable,
        cast(sum(rated_amount) as bigint) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROSS_ADD_SUBSCRIPTION'
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
        'Churn' axe_vue_transversale ,
         null axe_revenu,
        'CHURN' axe_subscriber,
        source_table,
        'SUM' cummulable,
        cast(sum(rated_amount) as bigint) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_CHURN'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL

    ------- Subscriber overview : Net adds
    select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI ,
        'Net adds' axe_vue_transversale ,
        null axe_revenu,
        'NET ADDS' axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum(parcj1-parcj0) valeur
    from (
        select region_id,cast(sum(rated_amount) as bigint) parcj0,max(source_table) source_table from  TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date= date_sub('2023-09-24',1)   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )a
    left join  (
        select region_id,cast(sum(rated_amount) as bigint) parcj1 from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date= '2023-09-24'    and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )b on a.region_id=b.region_id
    left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
    group by
    c.administrative_region ,
    c.commercial_region,
    source_table


    UNION ALL
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Subscriber overview' category,
        'Tx users (30jrs) en %' KPI ,
        'Tx users (30jrs) en %' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'MOY' cummulable,
        max(a.valeur/nvl(b.valeur,1))*100 valeur
    FROM (
        select sum(rated_amount) valeur,max(source_table) source_table
        from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
        where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'
    )a
    left join (
        SELECT sum(rated_amount) valeur,max(source_table) source_table
        FROM TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
        where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
    )b




    UNION ALL
    ------- Leviers de croissance : Revenue Data Mobile
    --TODO : faire la mise à jour du ref des subs et aligner le dwh et le dlk
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
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'REVENUE' AND sub_account='MAIN' and (DESTINATION_CODE='REVENUE_DATA_BUNDLE' or DESTINATION_CODE='OM_DATA' or (DESTINATION_CODE='REVENUE_DATA_PAYGO' and service_code<>'NVX_GPRS_SVA') or source_table in ('FT_EMERGENCY_DATA','FT_DATA_TRANSFER','FT_SUBS_RETAIL_ZEBRA'))
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Leviers de croissance : Price Per Megas
    --TODO: prendre en commpte les usages dans OTARIE
    select
         NULL region_administrative,
         NULL region_commerciale,
        'Leviers de croissance' category,
        'Price Per Megas' KPI ,
        'Price Per Megas' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(source_table) source_table,
        'MOY' cummulable,
        max(valeur) valeur 
        from (
            SELECT
                 cast(valeur_a/(valeur_b) as double) valeur,concat(a.source_table,'&',b.source_table) source_table
             from (
                SELECT
                 cast(sum(rated_amount) as double ) valeur_a,max(source_table) source_table
                 from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
                where transaction_date ='2023-09-24'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,12)='REVENUE_DATA' or DESTINATION_CODE='OM_DATA')
            )a
            left join (
                select
                cast(sum(rated_amount) as double )/1024/1024  valeur_b,max(source_table) source_table
                from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
                where transaction_date ='2023-09-24'   and KPI= 'USAGE'  and DESTINATION_CODE='USAGE_DATA_GPRS'

            )b
        )a

    UNION ALL
    ------- Leviers de croissance : Users (30jrs, >1Mo)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Data users (30jrs, >1Mo)' KPI ,
        'Data users (30jrs, >1Mo)' axe_vue_transversale ,
         null axe_revenu,
         NULL axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'OTARIE_DATA_USERS_30_DAYS_1Mo'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- Leviers de croissance : Users (MTD, >1Mo)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Data users (MTD, >1Mo)' KPI ,
        null axe_vue_transversale ,
         null axe_revenu,
        'Data users (MTD, >1Mo)' axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'OTARIE_DATA_USERS_MTD_1Mo'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

  UNION ALL
    ------- Leviers de croissance : Users (7jrs, >1Mo)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Data users (7jrs, >1Mo)' KPI ,
        null axe_vue_transversale ,
         null axe_revenu,
        'Data users (7jrs, >1Mo)' axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'OTARIE_DATA_USERS_7_DAYS_1Mo'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- Leviers de croissance : Users (Daily, >1Mo)
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Data users (Daily, >1Mo)' KPI ,
         null axe_vue_transversale ,
         null axe_revenu,
        'Data users (Daily, >1Mo)' axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'OTARIE_DATA_USERS_DAILY_1Mo'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- DAILY BASE
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Daily base' KPI ,
         null axe_vue_transversale ,
         null axe_revenu,
         null axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'PARC' and destination_code='USER_DAILY_ACTIVE'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- DAILY BASE
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Charge base' KPI ,
        'Charge base' axe_vue_transversale ,
         null axe_revenu,
         null axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'PARC' and destination_code='USER_30DAYS_GROUP'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Leviers de croissance : Tx users data
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Leviers de croissance' category,
        'Tx users data(30jrs) en %' KPI ,
        'Tx users data(30jrs) en %' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'MOY' cummulable,
        max(a.valeur/nvl(b.valeur,1))*100 valeur
    FROM (
        select sum(rated_amount) valeur,max(source_table) source_table
        from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
        where transaction_date ='2023-09-24'   and KPI= 'OTARIE_DATA_USERS_30_DAYS_1Mo'
    )a
    left join (
        SELECT sum(rated_amount) valeur,max(source_table) source_table
        FROM TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
        where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'
    )b


    UNION ALL
       ------- Leviers de croissance : Revenue Orange Money
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
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'REVENUE_OM'
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
        'Users OM (30jrs)' axe_vue_transversale ,
        null axe_revenu,
        'Subs OM' axe_subscriber,
        source_table source_table,
        'MAX' cummulable,
        sum(rated_amount)  valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'PARC_OM_30Jrs'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Leviers de croissance : Users (7jrs)
    select
         b.administrative_region region_administrative,
         b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users OM (7jrs)' KPI ,
        NULL axe_vue_transversale ,
        null axe_revenu,
        'Users OM (7jrs)' axe_subscriber,
        source_table source_table,
        'MAX' cummulable,
        sum(rated_amount)  valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'PARC_OM_7Jrs'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- Leviers de croissance : Users (Daily)
    select
         b.administrative_region region_administrative,
         b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users OM (Daily)' KPI ,
        NULL axe_vue_transversale ,
        null axe_revenu,
        'Users OM (Daily)' axe_subscriber,
        source_table source_table,
        'MAX' cummulable,
        sum(rated_amount)  valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'PARC_OM_DAILY'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

    UNION ALL
    ------- Leviers de croissance : Users (MTD)
    select
         b.administrative_region region_administrative,
         b.commercial_region region_commerciale,
        'Leviers de croissance' category,
        'Users OM (MTD)' KPI ,
        NULL axe_vue_transversale ,
        null axe_revenu,
        'Users OM (MTD)' axe_subscriber,
        source_table source_table,
        'MAX' cummulable,
        sum(rated_amount)  valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date = '2023-09-24'   and KPI= 'PARC_OM_MTD'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table

     union all
     select
         NULL region_administrative,
         NULL region_commerciale,
         'Leviers de croissance' category,
         'Tx users OM(30jrs) en %' KPI ,
         'Tx users OM(30jrs) en %' axe_vue_transversale ,
         null axe_revenu,
         null axe_subscriber,
         NULL source_table,
         'MOY' cummulable,
         max(a.valeur/nvl(b.valeur,1))*100 valeur
     FROM (
         select sum(rated_amount) valeur
         from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
         where transaction_date ='2023-09-24'   and KPI= 'PARC_OM_30Jrs'
     )a
     left join (
         SELECT sum(rated_amount) valeur
         FROM TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
         where transaction_date ='2023-09-24'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'
     )b
    UNION ALL
    ------- Leviers de croissance : Cash In Valeur
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
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'CASH_IN_OM'
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
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'CASH_OUT_OM'
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
        'Payments(Bill, Merch)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table source_table,
        'SUM' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI in( 'MERCH_PAY_OM','BILL_PAY_OM')
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


   UNION ALL
        ------- Distribution : Payments(Bill, Merch)
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Distribution' category,
        'Self Top UP ratio (%)' KPI ,
        'Self Top UP ratio (%)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        concat(a.source_table,'&',b.source_table) source_table,
        'MOY' cummulable,
        (a.rated_amount/b.rated_amount)*100 valeur
    from (
        select sum(rated_amount) rated_amount ,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date ='2023-09-24'   and KPI in ('REFILL_SELF_TOP','DATA_VIA_OM')
    ) a
    left join (
        select  sum(rated_amount) rated_amount,max(source_table) source_table   from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date ='2023-09-24'   and KPI= 'VALEUR_AIRTIME'
     )  b


  -- UNION ALL
    ----OK
    ------- Distribution : Recharges (C2S, CAG, OM)
--    select
--        b.administrative_region region_administrative,
--        b.commercial_region region_commerciale,
--        'Distribution' category,
--        'Recharges (C2S, CAG, OM)' KPI ,
--        'Recharges (C2S, CAG, OM)' axe_vue_transversale ,
--        null axe_revenu,
--        null axe_subscriber,
--        source_table,
--        'SUM' cummulable,
--        sum(rated_amount) valeur
--    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
--    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
--    where transaction_date ='2023-09-24'   and KPI= 'VALEUR_AIRTIME'
--    group by
--    b.administrative_region ,
--    b.commercial_region,
--    source_table


    UNION ALL
    ----OK
    ------- Distribution : Stock total client
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Stock total client(OM)' KPI ,
        'Stock total client(OM)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount) valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date =  '2023-09-24'   and KPI= 'BALANCE_OM'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table



    UNION ALL
    ---- OK
    ------- Distribution : Nombre de Pos Airtime actif
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Nombre de Pos Airtime actif(30jrs)' KPI ,
        'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount)  valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'POS_AIRTIME_30Js'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table




    UNION ALL
    ----OK
    ------- Distribution : Nombre de Pos OM actif
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Distribution' category,
        'Nombre de Pos OM actif(30jrs)' KPI ,
        'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'MAX' cummulable,
        sum(rated_amount)  valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date   ='2023-09-24' and KPI= 'PDV_OM_ACTIF_30Jrs'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table


    UNION ALL
    ------- Distribution : Niveau de stock @ retailer level (nb jour)
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Distribution' category,
        'Niveau de stock @ distributor level (nb jour)' KPI ,
        'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'WEEKLY' cummulable,
        sum(a.rated_amount)/sum(b.rated_amount) valeur
    from (
        select sum(rated_amount) rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date = '2023-09-24'   and KPI= 'SNAPSHOT_STOCK_DIST'
    ) a
    left join  (
        select avg(rated_amount) rated_amount,max(source_table) source_table from (
            select transaction_date ,sum(rated_amount)  rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date BETWEEN date_sub('2023-09-24',6) and '2023-09-24'   and KPI= 'AVG_REFILL_CLIENT'
         group by transaction_date
        ) a
    ) b
    union all
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Distribution' category,
        'Niveau de stock @ distributor level (nb jour)' KPI ,
        'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'MONTHLY' cummulable,
        sum(a.rated_amount)/sum(b.rated_amount) valeur
    from (
        select sum(rated_amount) rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date = '2023-09-24'   and KPI= 'SNAPSHOT_STOCK_DIST'
    ) a
    left join  (
        select avg(rated_amount) rated_amount,max(source_table) source_table from (
            select transaction_date ,sum(rated_amount)  rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date between CONCAT(SUBSTRING('2023-09-24',0,7),'-','01') and '2023-09-24'    and KPI= 'AVG_REFILL_CLIENT'
         group by transaction_date
        ) a
    ) b



    UNION ALL
    ------- Distribution : Niveau de stock @ retailer level (nb jour)
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Distribution' category,
        'Niveau de stock @ retailer level (nb jour)' KPI ,
        'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'WEEKLY' cummulable,
        sum(a.rated_amount)/sum(b.rated_amount) valeur
    from (
        select sum(rated_amount) rated_amount ,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date = '2023-09-24'   and KPI= 'SNAPSHOT_STOCK_CLIENT'
    ) a
    left join  (
        select avg(rated_amount) rated_amount,max(source_table) source_table from (
            select transaction_date ,sum(rated_amount)  rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date BETWEEN date_sub('2023-09-24',6) and '2023-09-24'   and KPI= 'AVG_REFILL_CLIENT'
         group by transaction_date
        ) a
    ) b
    union all
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Distribution' category,
        'Niveau de stock @ retailer level (nb jour)' KPI ,
        'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'MONTHLY' cummulable,
        sum(a.rated_amount)/sum(b.rated_amount) valeur
    from (
        select sum(rated_amount) rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date = '2023-09-24'   and KPI= 'SNAPSHOT_STOCK_CLIENT'
    ) a
    left join  (
        select avg(rated_amount) rated_amount,max(source_table) source_table from (
            select transaction_date ,sum(rated_amount)  rated_amount,max(source_table) source_table from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW where transaction_date between CONCAT(SUBSTRING('2023-09-24',0,7),'-','01') and '2023-09-24'    and KPI= 'AVG_REFILL_CLIENT'
         group by transaction_date
        ) a
    ) b





    UNION ALL
    ----TODO : partie Digital à traiter (Engagements)
    ------- Digital : Engagements
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Digital' category,
        'Engagements' KPI ,
        'Engagements' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        null valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'UNIQUE_DATA_USERS'
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
        'Pourcentage appel traité par BOT' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        null valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'UNIQUE_DATA_USERS'
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
        'Pourcentage traité en numérique' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        null valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'UNIQUE_DATA_USERS'
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
        'Durée moyenne de réponse numérique' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        source_table,
        'SUM' cummulable,
        null valeur
    from TT.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2023-09-24'   and KPI= 'UNIQUE_DATA_USERS'
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
    axe_subscriber,
    axe_revenu,
    axe_vue_transversale,
    cummulable