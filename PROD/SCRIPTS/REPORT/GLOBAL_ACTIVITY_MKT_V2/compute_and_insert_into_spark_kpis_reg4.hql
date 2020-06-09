INSERT INTO tmp.kpi_reg_final

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    null region_administrative,
    null region_commerciale,
    week.category,
    week.KPI,
    week.axe_revenue,
    week.axe_subscriber,
    week.axe_regionale,
    week.valeur,
    lweek.valeur  lweek,
    2wa.valeur 2wa,
    3wa.valeur 3wa,
    4wa.valeur 4wa,
    mtd.valeur mtd,
    lmtd.valeur lmtd,
    budget.valeur budget,
    budget_mtd.valeur budget_mtd,
    null mtd_last_year,
    if(lweek.valeur is null ,null,round((week.valeur-lweek.valeur)/lweek.valeur,2) ) vslweek,
    if(2wa.valeur is null ,null,round((week.valeur-2wa.valeur)/2wa.valeur,2))   vs2wa,
    if(3wa.valeur is null ,null,round((week.valeur-3wa.valeur)/3wa.valeur,2))   vs3wa,
    if(4wa.valeur is null ,null,round((week.valeur-4wa.valeur)/4wa.valeur,2) )  vs4wa,
    if(lmtd.valeur is null ,null,round((mtd.valeur-lmtd.valeur)/lmtd.valeur,2))   mtdvslmdt,
    if(budget_mtd.valeur is null ,null,round((mtd.valeur-budget_mtd.valeur)/budget_mtd.valeur,2)) mdtvsbudget,
    if(budget.valeur is null ,null,round((week.valeur-budget.valeur)/budget.valeur,2)) weekvsbudget,
    null   mtd_vs_last_year,
    '2020-05-12' processing_date
 from (
    select
    category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
     sum(valeur) valeur
     from  tmp.SPARK_KPIS_REG2
    where processing_date='2020-05-12' and granularite='WEEKLY'
    group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
    )week
 left join  (
 select
    category,
     KPI,
     axe_revenue,
     axe_subscriber,
      sum(valeur) valeur,
     axe_regionale from  tmp.SPARK_KPIS_REG2
  where processing_date=date_sub('2020-05-12',6) and granularite='WEEKLY'
   group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
 )lweek on nvl(week.category,'ND')=nvl(lweek.category,'ND') and nvl(week.KPI,'ND')=nvl(lweek.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(lweek.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(lweek.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(lweek.axe_regionale,'ND')
 left join  (
 select
  category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
    sum(valeur) valeur
       from  tmp.SPARK_KPIS_REG2
 where processing_date=date_sub('2020-05-12',14) and granularite='WEEKLY'
   group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
 )2wa on nvl(week.category,'ND')=nvl(2wa.category,'ND') and nvl(week.KPI,'ND')=nvl(2wa.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(2wa.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(2wa.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(2wa.axe_regionale,'ND')
 left join  (select
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
    sum(valeur) valeur
  from  tmp.SPARK_KPIS_REG2
 where processing_date=date_sub('2020-05-12',21) and granularite='WEEKLY'
   group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
 )3wa on  nvl(week.category,'ND')=nvl(3wa.category,'ND') and nvl(week.KPI,'ND')=nvl(3wa.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(3wa.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(3wa.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(3wa.axe_regionale,'ND')
 left join  (
 select
    category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
      sum(valeur) valeur
     from  tmp.SPARK_KPIS_REG2
 where processing_date=date_sub('2020-05-12',28) and granularite='WEEKLY'
   group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
 )4wa on  nvl(week.category,'ND')=nvl(4wa.category,'ND') and nvl(week.KPI,'ND')=nvl(4wa.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(4wa.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(4wa.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(4wa.axe_regionale,'ND')
 left join  (
   select
   category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale ,
      sum(valeur) valeur
     from  tmp.SPARK_KPIS_REG2
   where processing_date='2020-05-12' and granularite='MONTHLY'
     group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
 )mtd  on nvl(week.category,'ND')=nvl(mtd.category,'ND') and nvl(week.KPI,'ND')=nvl(mtd.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(mtd.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(mtd.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(mtd.axe_regionale,'ND')
 left join  (
   select
       category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
      sum(valeur) valeur
   from  tmp.SPARK_KPIS_REG2
    where processing_date=add_months('2020-05-12',-1) and granularite='MONTHLY'
      group by
     category,
     KPI,
     axe_revenue,
     axe_subscriber,
     axe_regionale
 )lmtd  on nvl(week.category,'ND')=nvl(lmtd.category,'ND') and nvl(week.KPI,'ND')=nvl(lmtd.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(lmtd.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(lmtd.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(lmtd.axe_regionale,'ND')

left join (

    select
        --b.administrative_region region_administrative,
        --b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI ,
        null axe_revenue,
        'CHURN' axe_subscriber,
        null axe_regionale,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between date_sub('2020-05-12',6) and '2020-05-12' )a
   -- left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    --group by
    --administrative_region,
    --commercial_region
    union all
    select
        --b.administrative_region region_administrative,
       -- b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI ,
        null axe_revenue,
        'GROSS ADDS' axe_subscriber,
        null axe_regionale,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between date_sub('2020-05-12',6) and '2020-05-12' )a
    --left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    --group by
    --administrative_region,
   -- commercial_region
     union all
    select
        --null  region_administrative,
       -- null region_commerciale,
         case
            when type in ('ca_global') then 'Revenue overview'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'Revenue overview'
            when type in ('data_combo','data_bundle','data_paygo') then 'Leviers de croissance'
            else 'Revenue overview'
        end   category,
        case
            when type in ('ca_global') then 'Telco (prepayé+hybrid) + OM'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'dont Voix'
            when type in ('data_combo','data_bundle','data_paygo') then 'Revenue Data Mobile'
            else null
        end kpi,
         null  axe_revenue,
          case
            when type in ('ca_global') then null
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'REVENU VOIX SORTANT'
            when type in ('data_combo','data_bundle','data_paygo') then null
            else null
        end  axe_subscriber,
         case
            when type in ('ca_global') then 'REVENUE TELCO (Prepaid+Hybrid+OM)'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then null
            when type in ('data_combo','data_bundle','data_paygo') then 'REVENU DATA'
            else null
        end  axe_regionale,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_GLOBAL where event_date between date_sub('2020-05-12',6) and '2020-05-12' )a
    group by
     case
            when type in ('ca_global') then 'Revenue overview'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'Revenue overview'
            when type in ('data_combo','data_bundle','data_paygo') then 'Leviers de croissance'
            else 'Revenue overview'
        end   ,
        case
            when type in ('ca_global') then 'Telco (prepayé+hybrid) + OM'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'dont Voix'
            when type in ('data_combo','data_bundle','data_paygo') then 'Revenue Data Mobile'
            else null
        end ,
          case
            when type in ('ca_global') then null
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'REVENU VOIX SORTANT'
            when type in ('data_combo','data_bundle','data_paygo') then null
            else null
        end  ,
         case
            when type in ('ca_global') then 'REVENUE TELCO (Prepaid+Hybrid+OM)'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then null
            when type in ('data_combo','data_bundle','data_paygo') then 'REVENU DATA'
            else null
        end
)budget on nvl(week.category,'ND')=nvl(budget.category,'ND') and nvl(week.KPI,'ND')=nvl(budget.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(budget.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(budget.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(budget.axe_regionale,'ND')
left join (

    select
        --b.administrative_region region_administrative,
       -- b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Churn' KPI ,
        null axe_revenue,
        'CHURN' axe_subscriber,
        null axe_regionale,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_DE where event_date between  CONCAT(SUBSTRING('2020-05-12',0,7),'-','01') and '2020-05-12' )a
    --left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    --group by
    --administrative_region,
    --commercial_region
    union all
    select
        --b.administrative_region region_administrative,
       -- b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI ,
        null axe_revenue,
        'GROSS ADDS' axe_subscriber,
        null axe_regionale,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_REGIONAL_GA where event_date between  CONCAT(SUBSTRING('2020-05-12',0,7),'-','01') and '2020-05-12' )a
    --left join dim.spark_dt_regions_mkt_v2 b on upper(a.type)=upper(b.administrative_region)
    --group by
    --administrative_region,
    --commercial_region
     union all
    select
        --null  region_administrative,
       -- null region_commerciale,
         case
            when type in ('ca_global') then 'Revenue overview'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'Revenue overview'
            when type in ('data_combo','data_bundle','data_paygo') then 'Leviers de croissance'
            else 'Revenue overview'
        end   category,
        case
            when type in ('ca_global') then 'Telco (prepayé+hybrid) + OM'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'dont Voix'
            when type in ('data_combo','data_bundle','data_paygo') then 'Revenue Data Mobile'
            else null
        end kpi,
         null  axe_revenue,
          case
            when type in ('ca_global') then null
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'REVENU VOIX SORTANT'
            when type in ('data_combo','data_bundle','data_paygo') then null
            else null
        end  axe_subscriber,
         case
            when type in ('ca_global') then 'REVENUE TELCO (Prepaid+Hybrid+OM)'
            when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then null
            when type in ('data_combo','data_bundle','data_paygo') then 'REVENU DATA'
            else null
        end  axe_regionale,
        sum(valeur) valeur
    from (select * from CDR.SPARK_TT_BUDGET_GLOBAL where event_date between  CONCAT(SUBSTRING('2020-05-12',0,7),'-','01') and '2020-05-12' )a
    group by
        case
                    when type in ('ca_global') then 'Revenue overview'
                    when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'Revenue overview'
                    when type in ('data_combo','data_bundle','data_paygo') then 'Leviers de croissance'
                    else 'Revenue overview'
                end   ,
                case
                    when type in ('ca_global') then 'Telco (prepayé+hybrid) + OM'
                    when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'dont Voix'
                    when type in ('data_combo','data_bundle','data_paygo') then 'Revenue Data Mobile'
                    else null
                end ,
                  case
                    when type in ('ca_global') then null
                    when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then 'REVENU VOIX SORTANT'
                    when type in ('data_combo','data_bundle','data_paygo') then null
                    else null
                end  ,
                 case
                    when type in ('ca_global') then 'REVENUE TELCO (Prepaid+Hybrid+OM)'
                    when type in ('ca_bundle','ca_paygo','sms_bundle','sms_paygo') then null
                    when type in ('data_combo','data_bundle','data_paygo') then 'REVENU DATA'
                    else null
                end
)budget_mtd on  nvl(week.category,'ND')=nvl(budget_mtd.category,'ND') and nvl(week.KPI,'ND')=nvl(budget_mtd.KPI,'ND') and nvl(week.axe_revenue,'ND')=nvl(budget_mtd.axe_revenue,'ND') and nvl(week.axe_subscriber,'ND')=nvl(budget_mtd.axe_subscriber,'ND') and nvl(week.axe_regionale,'ND')=nvl(budget_mtd.axe_regionale,'ND')
