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
    null mtd_last_year,
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
    null   mtd_vs_last_year,
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,        
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(budget_netadd) valeur
    from (select * from TMP.SPLIT_FINAL_BUDGET_NET_ADD where JOUR between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' )a
    group by
    region_administrative,
    region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,        
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC where jour ='###SLICE_VALUE###'
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
        sum(revenu_voix_sms_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
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
        sum(revenu_data_paygo_bundle_combo_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total) valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL where event_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,        
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(budget_jour_recharge2) valeur
    from TMP.SPLIT_FINAL_BUDGET_REFILL where jour between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu2) valeur
    from TMP.SPLIT_FINAL_BUDGET_OM where jour between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,        
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(budget_netadd) valeur
    from (select * from TMP.SPLIT_FINAL_BUDGET_NET_ADD where JOUR between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7) )a
    group by
    region_administrative,
    region_commerciale


    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,        
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC where jour=date_sub('###SLICE_VALUE###',7)
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
        sum(revenu_voix_sms_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
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
        sum(revenu_data_paygo_bundle_combo_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total) valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL where event_date between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,        
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(budget_jour_recharge2) valeur
    from TMP.SPLIT_FINAL_BUDGET_REFILL where jour between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu2) valeur
    from TMP.SPLIT_FINAL_BUDGET_OM where jour between date_sub('###SLICE_VALUE###',13) and date_sub('###SLICE_VALUE###',7)
    group by
        region_administrative,
        region_commerciale
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region


    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,        
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(budget_netadd) valeur
    from (select * from TMP.SPLIT_FINAL_BUDGET_NET_ADD where JOUR between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14) )a
    group by
    region_administrative,
    region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,        
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC where jour=date_sub('###SLICE_VALUE###',14)
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
        sum(revenu_voix_sms_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
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
        sum(revenu_data_paygo_bundle_combo_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total) valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL where event_date between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,        
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(budget_jour_recharge2) valeur
    from TMP.SPLIT_FINAL_BUDGET_REFILL where jour between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu2) valeur
    from TMP.SPLIT_FINAL_BUDGET_OM where jour between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
    group by
        region_administrative,
        region_commerciale
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region


    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,        
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(budget_netadd) valeur
    from (select * from TMP.SPLIT_FINAL_BUDGET_NET_ADD where JOUR between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21) )a
    group by
    region_administrative,
    region_commerciale


    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,        
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC where jour=date_sub('###SLICE_VALUE###',21)
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
        sum(revenu_voix_sms_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
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
        sum(revenu_data_paygo_bundle_combo_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total) valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL where event_date between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,        
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(budget_jour_recharge2) valeur
    from TMP.SPLIT_FINAL_BUDGET_REFILL where jour between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu2) valeur
    from TMP.SPLIT_FINAL_BUDGET_OM where jour between date_sub('###SLICE_VALUE###',27) and date_sub('###SLICE_VALUE###',21)
    group by
        region_administrative,
        region_commerciale
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
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
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28) )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region

    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,        
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(budget_netadd) valeur
    from (select * from TMP.SPLIT_FINAL_BUDGET_NET_ADD where JOUR between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28) )a
    group by
    region_administrative,
    region_commerciale
    union all
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,        
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC where jour=date_sub('###SLICE_VALUE###',28)
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
        sum(revenu_voix_sms_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
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
        sum(revenu_data_paygo_bundle_combo_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total) valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL where event_date between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,        
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(budget_jour_recharge2) valeur
    from TMP.SPLIT_FINAL_BUDGET_REFILL where jour between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu2) valeur
    from TMP.SPLIT_FINAL_BUDGET_OM where jour between date_sub('###SLICE_VALUE###',34) and date_sub('###SLICE_VALUE###',28)
    group by
        region_administrative,
        region_commerciale
)budget_4wa on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_4wa.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_4wa.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_4wa.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_4wa.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_4wa.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_4wa.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_4wa.axe_revenu,'ND'))


----- budget mtd
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
    select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between  CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###' )a
    left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    group by
    administrative_region,
    commercial_region
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

    UNION ALL
    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI,
        'Net adds' axe_vue_transversale,        
        'NET ADDS' axe_subscriber,
        null axe_revenu,
        sum(budget_netadd) valeur
    from (select * from TMP.SPLIT_FINAL_BUDGET_NET_ADD where JOUR between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###' )a
    group by
    region_administrative,
    region_commerciale

    union all

    select
        region_administrative,
        region_commerciale,
        'Subscriber overview' category,
        'Subscriber base' KPI,
        'Subscriber base' axe_vue_transversale,        
        'PARC 90 Jrs' axe_subscriber,
        null axe_revenu,
        sum(budget_jour_parc_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_PARC where jour ='###SLICE_VALUE###'
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
        sum(revenu_voix_sms_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_VOICE_SMS where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
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
        sum(revenu_data_paygo_bundle_combo_reg) valeur
    from TMP.SPLIT_FINAL_BUDGET_DATA where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'Telco (prepayé+hybrid) + OM'  KPI,
        'Telco (prepayé+hybrid) + OM'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENUE TELCO (Prepaid+Hybrid+OM)' axe_revenu,
        sum(budget_total) valeur
    from TMP.SPLIT_FINAL_BUDGET_GLOBAL where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Revenue overview'  category,
        'dont sortant (~recharges)'  KPI,
        'dont sortant (~recharges)'  axe_vue_transversale,        
        null axe_subscriber,
        'RECHARGE' axe_revenu,
        sum(budget_jour_recharge2) valeur
    from TMP.SPLIT_FINAL_BUDGET_REFILL where jour between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale

    union all
    select
        region_administrative,
        region_commerciale,
        'Leviers de croissance'  category,
        'Revenue Orange Money'  KPI,
        'Revenue Orange Money'  axe_vue_transversale,        
        null axe_subscriber,
        'REVENU OM' axe_revenu,
        sum(budget_jour_revenu2) valeur
    from TMP.SPLIT_FINAL_BUDGET_OM where jour between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
    group by
        region_administrative,
        region_commerciale
)budget_mtd on upper(nvl(week.region_administrative,'ND'))=upper(nvl(budget_mtd.region_administrative,'ND')) and upper(nvl(week.region_commerciale,'ND'))=upper(nvl(budget_mtd.region_commerciale,'ND')) and upper(nvl(week.category,'ND'))=upper(nvl(budget_mtd.category,'ND')) and upper(nvl(week.KPI,'ND'))=upper(nvl(budget_mtd.KPI,'ND')) and upper(nvl(week.axe_vue_transversale,'ND'))=upper(nvl(budget_mtd.axe_vue_transversale,'ND')) and upper(nvl(week.axe_subscriber,'ND'))=upper(nvl(budget_mtd.axe_subscriber,'ND')) and upper(nvl(week.axe_revenu,'ND'))=upper(nvl(budget_mtd.axe_revenu,'ND'))
