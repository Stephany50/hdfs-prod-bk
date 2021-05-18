insert into AGG.SPARK_KPIS_DG_FINAL

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    lower (week.region_administrative) region_administrative,
    lower(week.region_commerciale) region_commerciale ,
    week.category,
    week.KPI,
    week.axe_vue_transversale,
    week.axe_revenu,
    week.axe_subscriber,
    week.source_table,
    week.valeur,
    day.valeur valeur_day,
    lweek.valeur  lweek,
    2wa.valeur v2wa,
    3wa.valeur v3wa,
    4wa.valeur v4wa,
    mtd.valeur mtd,
    lmtd.valeur lmtd,
    budget.valeur budget,
    budget_lweek.valeur budget_lweek,
    budget_2wa.valeur budget_2wa,
    budget_3wa.valeur budget_3wa,
    budget_4wa.valeur budget_4wa,
    budget_mtd.valeur budget_mtd,
    lyear.valeur mtd_last_year,
    if(lweek.valeur is null ,null,round(((week.valeur-lweek.valeur)/lweek.valeur)*100,2) ) vslweek,
    if(2wa.valeur is null ,null,round(((week.valeur-2wa.valeur)/2wa.valeur)*100,2))   vs2wa,
    if(3wa.valeur is null ,null,round(((week.valeur-3wa.valeur)/3wa.valeur)*100,2))   vs3wa,
    if(4wa.valeur is null ,null,round(((week.valeur-4wa.valeur)/4wa.valeur)*100,2) )  vs4wa,
    if(lmtd.valeur is null ,null,round(((mtd.valeur-lmtd.valeur)/lmtd.valeur)*100,2))   mtdvslmdt,
    if(budget_mtd.valeur is null ,null,round(((mtd.valeur-budget_mtd.valeur)/budget_mtd.valeur)*100,2)) mdtvsbudget,
    if(budget.valeur is null ,null,round(((week.valeur-budget.valeur)/budget.valeur)*100,2)) weekvsbudget,
    if(budget_lweek.valeur is null ,null,round(((lweek.valeur-budget_lweek.valeur)/budget_lweek.valeur)*100,2)) lweekvsblweek,
    if(budget_2wa.valeur is null ,null,round(((2wa.valeur-budget_2wa.valeur)/budget_2wa.valeur)*100,2)) v2wavsb2wa,
    if(budget_3wa.valeur is null ,null,round(((3wa.valeur-budget_3wa.valeur)/budget_3wa.valeur)*100,2)) v3wavsb3wa,
    if(budget_4wa.valeur is null ,null,round(((4wa.valeur-budget_4wa.valeur)/budget_4wa.valeur)*100,2)) v4wavsb4wa,
    if(lyear.valeur is null , null,round(((mtd.valeur-lyear.valeur)/lyear.valeur)*100,2))   mtd_vs_last_year,
    'ADMINISTRATIVE_REGION' granularite_reg,
    current_timestamp insert_date,
    week.processing_date
     from (select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date='###SLICE_VALUE###' and granularite='WEEKLY')week
 left join (select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date='###SLICE_VALUE###' and granularite='DAILY')day on upper(nvl(week.region_administrative,'ND'))=upper(nvl(day.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(day.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(day.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(day.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(day.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(day.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(day.axe_revenu,'ND'))
 left join  (select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date=date_sub('###SLICE_VALUE###',6) and granularite='WEEKLY' )lweek on upper(nvl(week.region_administrative,'ND'))=upper(nvl(lweek.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(lweek.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(lweek.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(lweek.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(lweek.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(lweek.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(lweek.axe_revenu,'ND'))
 left join  (select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date=date_sub('###SLICE_VALUE###',14) and granularite='WEEKLY')2wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(2wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(2wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(2wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(2wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(2wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(2wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(2wa.axe_revenu,'ND'))
 left join  (select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date=date_sub('###SLICE_VALUE###',21) and granularite='WEEKLY')3wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(3wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(3wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(3wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(3wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(3wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(3wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(3wa.axe_revenu,'ND'))
 left join  (select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date=date_sub('###SLICE_VALUE###',28) and granularite='WEEKLY')4wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(4wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(4wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(4wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(4wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(4wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(4wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(4wa.axe_revenu,'ND'))
 left join  (
   select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date='###SLICE_VALUE###' and granularite='MONTHLY'
 )mtd  on upper(nvl(week.region_administrative,'ND'))=upper(nvl(mtd.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(mtd.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(mtd.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(mtd.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(mtd.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(mtd.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(mtd.axe_revenu,'ND'))
 left join  (
   select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date=add_months('###SLICE_VALUE###',-1) and granularite='MONTHLY'
 )lmtd  on upper(nvl(week.region_administrative,'ND'))=upper(nvl(lmtd.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(lmtd.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(lmtd.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(lmtd.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(lmtd.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(lmtd.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(lmtd.axe_revenu,'ND'))
 left join  (
   select * from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date=add_months('###SLICE_VALUE###',-12) and granularite='MONTHLY'
 )lyear  on upper(nvl(week.region_administrative,'ND'))=upper(nvl(lyear.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(lyear.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(lyear.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(lyear.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(lyear.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(lyear.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(lyear.axe_revenu,'ND'))

left join (
----OK
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI,
        'Churn' axe_vue_transversale,
        'CHURN' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE2 where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.region)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

----OK
    union all
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI,
        'Gross Adds' axe_vue_transversale,
        'GROSS ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date  between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
----OK
    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
         select
            a.jour jour,
            a.region_administrative,
            a.region_commerciale  ,
            parc_act- parc_prec valeur
         from (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_act
             from TMP.SPLIT_FINAL_BUDGET_PARC2
             where jour ='###SLICE_VALUE###'
             group by  jour,region_administrative,region_commerciale
         ) a left join (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_prec
            from TMP.SPLIT_FINAL_BUDGET_PARC2
            where jour =add_months('###SLICE_VALUE###',-1)
            group by  jour,region_administrative,region_commerciale ) b on nvl(a.jour,'ND')=nvl(add_months(b.jour,1),'ND') and upper(nvl(a.region_administrative,'ND'))=upper(nvl(b.region_administrative,'ND')) and upper(nvl(a.region_commerciale,'ND'))=upper(nvl(b.region_commerciale,'ND'))
     )a
    group by
    region_administrative,
    region_commerciale

    union all
----OK
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC2 where jour ='###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

------ OKK POUR LA VOIX
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(revenu_voix_sms_reg)*1.1925*1.02  valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS5 where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(valeur) *1.1925*1.02  valeur
     from tmp.budget_voix  where event_date  between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
      group by
            region_administrative,
            region_commerciale

---DATA OK
    union all


    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Data Mobile'  KPI,
        'Revenue Data Mobile'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU DATA' axe_revenu,
        sum(revenu_data_paygo_bundle_combo_reg)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA3 where event_date  between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
      select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) *1.1925*1.02 valeur
       from tmp.budget_data2 where   event_date  between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
        group by
               region_administrative,
               region_commerciale

-- OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL3 where event_date  between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale
--OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum (valeur)*1.1925*1.02 valeur
    from (
      select
            upper(region_administrative)region_administrative,
            upper(region_commerciale) region_commerciale,
            sum(valeur) valeur
         from tmp.budget_sortant2  where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
         group by
            upper(region_administrative),
            upper(region_commerciale)
         union all
         select
              upper(region_administrative)region_administrative,
              upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
             sum(budget_jour_revenu) valeur
          from  TMP.SPLIT_FINAL_BUDGET_OM5 where jour>="2020-10-01" and  jour  between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
          group by
                 upper(region_administrative),
                 upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )
      )d
        group by
               region_administrative,
               region_commerciale

--OK POUR LES RECHARGES
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(valeur) valeur
    from  (
--            select
--                jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(budget_jour_recharge2 ) valeur
--             from  TMP.SPLIT_FINAL_BUDGET_REFILL where  jour <="2020-09-30"
--             group by  jour,upper(region_administrative)  ,upper(region_commerciale)
--             union all
             select
                event_date jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(valeur )*1.1925*1.02*0.95 valeur
             from  tmp.budget_sortant2  where  event_date>="2020-10-01"
             group by  event_date,upper(region_administrative)  ,upper(region_commerciale)
     ) a where jour between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all

---OK POUR OM
    select
        upper(region_administrative)region_administrative,
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_OM5 where jour between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        upper(region_administrative),
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )

    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ distributor level (nb jour)' KPI ,
            'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_distributor_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ retailer level (nb jour)' KPI ,
            'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_retailer_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos Airtime actif(30jrs)' KPI ,
             'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(airtime_pos_actif)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos OM actif(30jrs)' KPI ,
             'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(om_pos)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'INCONNU' region_administrative,
             'INCONNU' region_commerciale,
             'Distribution' category,
            'Self Top UP ratio (%)' KPI ,
            'Self Top UP ratio (%)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             max(self_refill_per)*100  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

)budget on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget.axe_revenu,'ND'))

---- budget_lw
left join (

      select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI,
        'Churn' axe_vue_transversale,
        'CHURN' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE2 where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.region)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

----OK
    union all
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI,
        'Gross Adds' axe_vue_transversale,
        'GROSS ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date  between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
----OK
    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
         select
            a.jour jour,
            a.region_administrative,
            a.region_commerciale  ,
            parc_act- parc_prec valeur
         from (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_act
             from TMP.SPLIT_FINAL_BUDGET_PARC2
             where jour ='###SLICE_VALUE###'
             group by  jour,region_administrative,region_commerciale
         ) a left join (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_prec
            from TMP.SPLIT_FINAL_BUDGET_PARC2
            where jour =add_months('###SLICE_VALUE###',-1)
            group by  jour,region_administrative,region_commerciale ) b on nvl(a.jour,'ND')=nvl(add_months(b.jour,1),'ND') and upper(nvl(a.region_administrative,'ND'))=upper(nvl(b.region_administrative,'ND')) and upper(nvl(a.region_commerciale,'ND'))=upper(nvl(b.region_commerciale,'ND'))
     )a
    group by
    region_administrative,
    region_commerciale

    union all
----OK
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC2 where jour ='###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

------ OKK POUR LA VOIX
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(revenu_voix_sms_reg)*1.1925*1.02  valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS5 where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(valeur) *1.1925*1.02  valeur
     from tmp.budget_voix  where event_date  between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
      group by
            region_administrative,
            region_commerciale

---DATA OK
    union all


    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Data Mobile'  KPI,
        'Revenue Data Mobile'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU DATA' axe_revenu,
        sum(revenu_data_paygo_bundle_combo_reg)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA3 where event_date  between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale

    union all
      select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) *1.1925*1.02 valeur
       from tmp.budget_data2 where   event_date  between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
        group by
               region_administrative,
               region_commerciale

-- OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL3 where event_date  between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale
--OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum (valeur)*1.1925*1.02 valeur
    from (
      select
            upper(region_administrative)region_administrative,
            upper(region_commerciale) region_commerciale,
            sum(valeur) valeur
         from tmp.budget_sortant2  where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
         group by
            upper(region_administrative),
            upper(region_commerciale)
         union all
         select
              upper(region_administrative)region_administrative,
              upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
             sum(budget_jour_revenu) valeur
          from  TMP.SPLIT_FINAL_BUDGET_OM5 where jour>="2020-10-01" and  jour  between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
          group by
                 upper(region_administrative),
                 upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )
      )d
        group by
               region_administrative,
               region_commerciale

--OK POUR LES RECHARGES
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(valeur) valeur
    from  (
--            select
--                jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(budget_jour_recharge2 ) valeur
--             from  TMP.SPLIT_FINAL_BUDGET_REFILL where  jour <="2020-09-30"
--             group by  jour,upper(region_administrative)  ,upper(region_commerciale)
--             union all
             select
                event_date jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(valeur )*1.1925*1.02*0.95 valeur
             from  tmp.budget_sortant2  where  event_date>="2020-10-01"
             group by  event_date,upper(region_administrative)  ,upper(region_commerciale)
     ) a where jour between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale

    union all

---OK POUR OM
    select
        upper(region_administrative)region_administrative,
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_OM5 where jour between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        upper(region_administrative),
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )

    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ distributor level (nb jour)' KPI ,
            'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_distributor_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ retailer level (nb jour)' KPI ,
            'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_retailer_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos Airtime actif(30jrs)' KPI ,
             'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(airtime_pos_actif)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos OM actif(30jrs)' KPI ,
             'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(om_pos)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'INCONNU' region_administrative,
             'INCONNU' region_commerciale,
             'Distribution' category,
            'Self Top UP ratio (%)' KPI ,
            'Self Top UP ratio (%)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             max(self_refill_per)*100  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

)budget_lweek on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_lweek.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_lweek.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_lweek.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_lweek.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_lweek.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_lweek.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_lweek.axe_revenu,'ND'))




---- budget_2wa
left join (

      select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI,
        'Churn' axe_vue_transversale,
        'CHURN' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE2 where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.region)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

----OK
    union all
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI,
        'Gross Adds' axe_vue_transversale,
        'GROSS ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
----OK
    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
         select
            a.jour jour,
            a.region_administrative,
            a.region_commerciale  ,
            parc_act- parc_prec valeur
         from (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_act
             from TMP.SPLIT_FINAL_BUDGET_PARC2
             where jour ='###SLICE_VALUE###'
             group by  jour,region_administrative,region_commerciale
         ) a left join (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_prec
            from TMP.SPLIT_FINAL_BUDGET_PARC2
            where jour =add_months('###SLICE_VALUE###',-1)
            group by  jour,region_administrative,region_commerciale ) b on nvl(a.jour,'ND')=nvl(add_months(b.jour,1),'ND') and upper(nvl(a.region_administrative,'ND'))=upper(nvl(b.region_administrative,'ND')) and upper(nvl(a.region_commerciale,'ND'))=upper(nvl(b.region_commerciale,'ND'))
     )a
    group by
    region_administrative,
    region_commerciale

    union all
----OK
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC2 where jour ='###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

------ OKK POUR LA VOIX
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(revenu_voix_sms_reg)*1.1925*1.02  valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS5 where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(valeur) *1.1925*1.02  valeur
     from tmp.budget_voix  where event_date  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
      group by
            region_administrative,
            region_commerciale

---DATA OK
    union all


    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Data Mobile'  KPI,
        'Revenue Data Mobile'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU DATA' axe_revenu,
        sum(revenu_data_paygo_bundle_combo_reg)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA3 where event_date  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale

    union all
      select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) *1.1925*1.02 valeur
       from tmp.budget_data2 where   event_date  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
        group by
               region_administrative,
               region_commerciale

-- OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL3 where event_date  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale
--OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum (valeur)*1.1925*1.02 valeur
    from (
      select
            upper(region_administrative)region_administrative,
            upper(region_commerciale) region_commerciale,
            sum(valeur) valeur
         from tmp.budget_sortant2  where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
         group by
            upper(region_administrative),
            upper(region_commerciale)
         union all
         select
              upper(region_administrative)region_administrative,
              upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
             sum(budget_jour_revenu) valeur
          from  TMP.SPLIT_FINAL_BUDGET_OM5 where jour>="2020-10-01" and  jour  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
          group by
                 upper(region_administrative),
                 upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )
      )d
        group by
               region_administrative,
               region_commerciale

--OK POUR LES RECHARGES
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(valeur) valeur
    from  (
--            select
--                jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(budget_jour_recharge2 ) valeur
--             from  TMP.SPLIT_FINAL_BUDGET_REFILL where  jour <="2020-09-30"
--             group by  jour,upper(region_administrative)  ,upper(region_commerciale)
--             union all
             select
                event_date jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(valeur )*1.1925*1.02*0.95 valeur
             from  tmp.budget_sortant2  where  event_date>="2020-10-01"
             group by  event_date,upper(region_administrative)  ,upper(region_commerciale)
     ) a where jour between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale

    union all

---OK POUR OM
    select
        upper(region_administrative)region_administrative,
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_OM5 where jour between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        upper(region_administrative),
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )

    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ distributor level (nb jour)' KPI ,
            'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_distributor_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ retailer level (nb jour)' KPI ,
            'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_retailer_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos Airtime actif(30jrs)' KPI ,
             'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(airtime_pos_actif)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos OM actif(30jrs)' KPI ,
             'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(om_pos)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'INCONNU' region_administrative,
             'INCONNU' region_commerciale,
             'Distribution' category,
            'Self Top UP ratio (%)' KPI ,
            'Self Top UP ratio (%)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             max(self_refill_per)*100  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

)budget_2wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_2wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_2wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_2wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_2wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_2wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_2wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_2wa.axe_revenu,'ND'))




---- budget_3wa
left join (

     select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI,
        'Churn' axe_vue_transversale,
        'CHURN' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE2 where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.region)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

----OK
    union all
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI,
        'Gross Adds' axe_vue_transversale,
        'GROSS ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date  between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
----OK
    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
         select
            a.jour jour,
            a.region_administrative,
            a.region_commerciale  ,
            parc_act- parc_prec valeur
         from (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_act
             from TMP.SPLIT_FINAL_BUDGET_PARC2
             where jour ='###SLICE_VALUE###'
             group by  jour,region_administrative,region_commerciale
         ) a left join (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_prec
            from TMP.SPLIT_FINAL_BUDGET_PARC2
            where jour =add_months('###SLICE_VALUE###',-1)
            group by  jour,region_administrative,region_commerciale ) b on nvl(a.jour,'ND')=nvl(add_months(b.jour,1),'ND') and upper(nvl(a.region_administrative,'ND'))=upper(nvl(b.region_administrative,'ND')) and upper(nvl(a.region_commerciale,'ND'))=upper(nvl(b.region_commerciale,'ND'))
     )a
    group by
    region_administrative,
    region_commerciale

    union all
----OK
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC2 where jour ='###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

------ OKK POUR LA VOIX
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(revenu_voix_sms_reg)*1.1925*1.02  valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS5 where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(valeur) *1.1925*1.02  valeur
     from tmp.budget_voix  where event_date  between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
      group by
            region_administrative,
            region_commerciale

---DATA OK
    union all


    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Data Mobile'  KPI,
        'Revenue Data Mobile'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU DATA' axe_revenu,
        sum(revenu_data_paygo_bundle_combo_reg)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA3 where event_date  between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale

    union all
      select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) *1.1925*1.02 valeur
       from tmp.budget_data2 where   event_date  between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
        group by
               region_administrative,
               region_commerciale

-- OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL3 where event_date  between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale
--OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum (valeur)*1.1925*1.02 valeur
    from (
      select
            upper(region_administrative)region_administrative,
            upper(region_commerciale) region_commerciale,
            sum(valeur) valeur
         from tmp.budget_sortant2  where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
         group by
            upper(region_administrative),
            upper(region_commerciale)
         union all
         select
              upper(region_administrative)region_administrative,
              upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
             sum(budget_jour_revenu) valeur
          from  TMP.SPLIT_FINAL_BUDGET_OM5 where jour>="2020-10-01" and  jour  between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
          group by
                 upper(region_administrative),
                 upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )
      )d
        group by
               region_administrative,
               region_commerciale

--OK POUR LES RECHARGES
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(valeur) valeur
    from  (
--            select
--                jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(budget_jour_recharge2 ) valeur
--             from  TMP.SPLIT_FINAL_BUDGET_REFILL where  jour <="2020-09-30"
--             group by  jour,upper(region_administrative)  ,upper(region_commerciale)
--             union all
             select
                event_date jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(valeur )*1.1925*1.02*0.95 valeur
             from  tmp.budget_sortant2  where  event_date>="2020-10-01"
             group by  event_date,upper(region_administrative)  ,upper(region_commerciale)
     ) a where jour between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale

    union all

---OK POUR OM
    select
        upper(region_administrative)region_administrative,
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_OM5 where jour between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        upper(region_administrative),
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )

    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ distributor level (nb jour)' KPI ,
            'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_distributor_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ retailer level (nb jour)' KPI ,
            'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_retailer_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos Airtime actif(30jrs)' KPI ,
             'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(airtime_pos_actif)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos OM actif(30jrs)' KPI ,
             'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(om_pos)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'INCONNU' region_administrative,
             'INCONNU' region_commerciale,
             'Distribution' category,
            'Self Top UP ratio (%)' KPI ,
            'Self Top UP ratio (%)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             max(self_refill_per)*100  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
)budget_3wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_3wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_3wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_3wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_3wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_3wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_3wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_3wa.axe_revenu,'ND'))

---- budget_4wa
left join (

      select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI,
        'Churn' axe_vue_transversale,
        'CHURN' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE2 where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.region)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

----OK
    union all
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI,
        'Gross Adds' axe_vue_transversale,
        'GROSS ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date  between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
----OK
    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
         select
            a.jour jour,
            a.region_administrative,
            a.region_commerciale  ,
            parc_act- parc_prec valeur
         from (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_act
             from TMP.SPLIT_FINAL_BUDGET_PARC2
             where jour ='###SLICE_VALUE###'
             group by  jour,region_administrative,region_commerciale
         ) a left join (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_prec
            from TMP.SPLIT_FINAL_BUDGET_PARC2
            where jour =add_months('###SLICE_VALUE###',-1)
            group by  jour,region_administrative,region_commerciale ) b on nvl(a.jour,'ND')=nvl(add_months(b.jour,1),'ND') and upper(nvl(a.region_administrative,'ND'))=upper(nvl(b.region_administrative,'ND')) and upper(nvl(a.region_commerciale,'ND'))=upper(nvl(b.region_commerciale,'ND'))
     )a
    group by
    region_administrative,
    region_commerciale

    union all
----OK
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC2 where jour ='###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

------ OKK POUR LA VOIX
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(revenu_voix_sms_reg)*1.1925*1.02  valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS5 where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(valeur) *1.1925*1.02  valeur
     from tmp.budget_voix  where event_date  between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
      group by
            region_administrative,
            region_commerciale

---DATA OK
    union all


    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Data Mobile'  KPI,
        'Revenue Data Mobile'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU DATA' axe_revenu,
        sum(revenu_data_paygo_bundle_combo_reg)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA3 where event_date  between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale

    union all
      select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) *1.1925*1.02 valeur
       from tmp.budget_data2 where   event_date  between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
        group by
               region_administrative,
               region_commerciale

-- OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL3 where event_date  between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale
--OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum (valeur)*1.1925*1.02 valeur
    from (
      select
            upper(region_administrative)region_administrative,
            upper(region_commerciale) region_commerciale,
            sum(valeur) valeur
         from tmp.budget_sortant2  where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
         group by
            upper(region_administrative),
            upper(region_commerciale)
         union all
         select
              upper(region_administrative)region_administrative,
              upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
             sum(budget_jour_revenu) valeur
          from  TMP.SPLIT_FINAL_BUDGET_OM5 where jour>="2020-10-01" and  jour  between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
          group by
                 upper(region_administrative),
                 upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )
      )d
        group by
               region_administrative,
               region_commerciale

--OK POUR LES RECHARGES
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(valeur) valeur
    from  (
--            select
--                jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(budget_jour_recharge2 ) valeur
--             from  TMP.SPLIT_FINAL_BUDGET_REFILL where  jour <="2020-09-30"
--             group by  jour,upper(region_administrative)  ,upper(region_commerciale)
--             union all
             select
                event_date jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(valeur )*1.1925*1.02*0.95 valeur
             from  tmp.budget_sortant2  where  event_date>="2020-10-01"
             group by  event_date,upper(region_administrative)  ,upper(region_commerciale)
     ) a where jour between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale

    union all

---OK POUR OM
    select
        upper(region_administrative)region_administrative,
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_OM5 where jour between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        upper(region_administrative),
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )

    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ distributor level (nb jour)' KPI ,
            'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_distributor_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ retailer level (nb jour)' KPI ,
            'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_retailer_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos Airtime actif(30jrs)' KPI ,
             'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(airtime_pos_actif)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos OM actif(30jrs)' KPI ,
             'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(om_pos)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'INCONNU' region_administrative,
             'INCONNU' region_commerciale,
             'Distribution' category,
            'Self Top UP ratio (%)' KPI ,
            'Self Top UP ratio (%)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             max(self_refill_per)*100  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
)budget_4wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_4wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_4wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_4wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_4wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_4wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_4wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_4wa.axe_revenu,'ND'))


----- budget mtd
left join (
----OK
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI,
        'Churn' axe_vue_transversale,
        'CHURN' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE2 where event_date between  CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.region)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

----OK
    union all
    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI,
        'Gross Adds' axe_vue_transversale,
        'GROSS ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between  CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
----OK
    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(valeur) valeur
    from (
         select
            a.jour jour,
            a.region_administrative,
            a.region_commerciale  ,
            parc_act- parc_prec valeur
         from (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_act
             from TMP.SPLIT_FINAL_BUDGET_PARC2
             where jour ='###SLICE_VALUE###'
             group by  jour,region_administrative,region_commerciale
         ) a left join (
            select
                jour,
                region_administrative,
                region_commerciale  ,
                sum(budget_jour_parc_reg) parc_prec
            from TMP.SPLIT_FINAL_BUDGET_PARC2
            where jour =add_months('###SLICE_VALUE###',-1)
            group by  jour,region_administrative,region_commerciale ) b on nvl(a.jour,'ND')=nvl(add_months(b.jour,1),'ND') and upper(nvl(a.region_administrative,'ND'))=upper(nvl(b.region_administrative,'ND')) and upper(nvl(a.region_commerciale,'ND'))=upper(nvl(b.region_commerciale,'ND'))
     )a
    group by
    region_administrative,
    region_commerciale

    union all
----OK
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC2 where jour ='###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

------ OKK POUR LA VOIX
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(revenu_voix_sms_reg)*1.1925*1.02  valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS5 where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont Voix'  KPI,
        'dont Voix'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU VOIX SORTANT' axe_revenu,
        sum(valeur) *1.1925*1.02  valeur
     from tmp.budget_voix  where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
      group by
            region_administrative,
            region_commerciale

---DATA OK
    union all


    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Data Mobile'  KPI,
        'Revenue Data Mobile'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU DATA' axe_revenu,
        sum(revenu_data_paygo_bundle_combo_reg)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA3 where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
      select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) *1.1925*1.02 valeur
       from tmp.budget_data2 where   event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
        group by
               region_administrative,
               region_commerciale

-- OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL3 where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale
--OK
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum (valeur)*1.1925*1.02 valeur
    from (
      select
            upper(region_administrative)region_administrative,
            upper(region_commerciale) region_commerciale,
            sum(valeur) valeur
         from tmp.budget_sortant2  where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
         group by
            upper(region_administrative),
            upper(region_commerciale)
         union all
         select
              upper(region_administrative)region_administrative,
              upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
             sum(budget_jour_revenu) valeur
          from  TMP.SPLIT_FINAL_BUDGET_OM5 where jour>="2020-10-01" and  jour between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
          group by
                 upper(region_administrative),
                 upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )
      )d
        group by
               region_administrative,
               region_commerciale

--OK POUR LES RECHARGES
    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(valeur) valeur
    from  (
--            select
--                jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(budget_jour_recharge2 ) valeur
--             from  TMP.SPLIT_FINAL_BUDGET_REFILL where  jour <="2020-09-30"
--             group by  jour,upper(region_administrative)  ,upper(region_commerciale)
--             union all
             select
                event_date jour,upper(region_administrative) region_administrative ,upper(region_commerciale) region_commerciale,sum(valeur )*1.1925*1.02*0.95 valeur
             from  tmp.budget_sortant2  where  event_date>="2020-10-01"
             group by  event_date,upper(region_administrative)  ,upper(region_commerciale)
     ) a where jour between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all

---OK POUR OM
    select
        upper(region_administrative)region_administrative,
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end ) region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu)*1.1925*1.02 valeur
    from TMP.SPLIT_FINAL_BUDGET_OM5 where jour between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        upper(region_administrative),
        upper(case when upper(region_administrative) ='SUD' then 'CENTRE - SUD - EST' else  region_commerciale end )

    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ distributor level (nb jour)' KPI ,
            'Niveau de stock @ distributor level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_distributor_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)
    union all

     select
            'INCONNU' region_administrative,
            'INCONNU' region_commerciale,
            'Distribution' category,
            'Niveau de stock @ retailer level (nb jour)' KPI ,
            'Niveau de stock @ retailer level (nb jour)' axe_vue_transversale ,
            null axe_subscriber,
            null axe_revenu,
            max(airtime_retailer_level_en_jours) valeur
            from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos Airtime actif(30jrs)' KPI ,
             'Nombre de Pos Airtime actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(airtime_pos_actif)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


     union all
        select
             'nord' region_administrative,
             'grand nord' region_commerciale,
             'Distribution' category,
             'Nombre de Pos OM actif(30jrs)' KPI ,
             'Nombre de Pos OM actif(30jrs)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             sum(om_pos)  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)

     union all
        select
             'INCONNU' region_administrative,
             'INCONNU' region_commerciale,
             'Distribution' category,
            'Self Top UP ratio (%)' KPI ,
            'Self Top UP ratio (%)' axe_vue_transversale ,
              null axe_subscriber,
             null axe_revenu,
             max(self_refill_per)*100  valeur
       from tmp.budget_kpi_dd where month=SUBSTRING('###SLICE_VALUE###',0,7)


)budget_mtd on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_mtd.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_mtd.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_mtd.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_mtd.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_mtd.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_mtd.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_mtd.axe_revenu,'ND'))
