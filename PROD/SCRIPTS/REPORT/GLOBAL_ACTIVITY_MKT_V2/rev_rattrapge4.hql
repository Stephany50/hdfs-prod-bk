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
                 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
                where transaction_date ='2020-12-20'   and KPI= 'REVENUE' AND sub_account='MAIN' and (SUBSTRING(DESTINATION_CODE,1,12)='REVENUE_DATA' or DESTINATION_CODE='OM_DATA')
            )a
            left join (
                select
                cast(sum(rated_amount) as double )/1024/1024  valeur_b,max(source_table) source_table
                from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
                where transaction_date ='2020-12-20'   and KPI= 'USAGE'  and DESTINATION_CODE='USAGE_DATA_GPRS'

            )b
        )a