

select
    region_administrative,
    region_commerciale,
    category,
    kpi,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    source_table,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur*(1-0.2125) else valeur end) valeur,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_day*(1-0.2125) else valeur_day end) valeur_day,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_lweek*(1-0.2125) else valeur_lweek end) valeur_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then v2wa*(1-0.2125) else v2wa end) v2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then v3wa*(1-0.2125) else v3wa end) v3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then v4wa*(1-0.2125) else v4wa end) v4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_mtd*(1-0.2125) else valeur_mtd end) valeur_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_lmtd*(1-0.2125) else valeur_lmtd end) valeur_lmtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget*(1-0.2125) else budget end) budget,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_lweek*(1-0.2125) else budget_lweek end) budget_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_2wa*(1-0.2125) else budget_2wa end) budget_2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_3wa*(1-0.2125) else budget_3wa end) budget_3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_4wa*(1-0.2125) else budget_4wa end) budget_4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_budget_mtd*(1-0.2125) else valeur_budget_mtd end) valeur_budget_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_mtd_last_year*(1-0.2125) else valeur_mtd_last_year end) valeur_mtd_last_year,
    vslweek,
    vs2wa,
    vs3wa,
    vs4wa,
    mtdvslmdt,
    mdtvsbudget,
    weekvsbudget,
    lweekvsblweek,
    v2wavsb2wa,
    v3wavsb3wa,
    v4wavsb4wa,
    mtd_vs_last_year,
    granularite_reg,
    insert_date,
    processing_date
 from AGG.SPARK_KPIS_DG_FINAL

create table mon.SPARK_KPIS_REG_FINAL5(
 region_administrative   varchar(100)            ,
 region_commerciale      varchar(100)            ,
 category                varchar(100)            ,
 kpi                     varchar(100)            ,
 axe_vue_transversale             varchar(100)            ,
 axe_revenu            varchar(100)            ,
 axe_subscriber          varchar(100)            ,
 source_table            varchar(100)            ,
 valeur                  double                  ,
 valeur_lweek            double                  ,
 v2wa                    double                  ,
 v3wa                    double                  ,
 v4wa                    double                  ,
 valeur_mtd              double                  ,
 valeur_lmtd             double                  ,
 budget                  double                  ,
 budget_lweek            double                  ,
 budget_2wa              double                  ,
 budget_3wa              double                  ,
 budget_4wa              double                  ,
 valeur_budget_mtd       double                  ,
 valeur_mtd_last_year    double                  ,
 vslweek                 double                  ,
 vs2wa                   double                  ,
 vs3wa                   double                  ,
 vs4wa                   double                  ,
 mtdvslmdt               double                  ,
 mdtvsbudget             double                  ,
 weekvsbudget            double                  ,
 lweekvsblweek           double                  ,
 v2wavsb2wa              double                  ,
 v3wavsb3wa              double                  ,
 v4wavsb4wa              double                  ,
 mtd_vs_last_year        double                  ,
 insert_date             timestamp               ,
 processing_date         date                    
) ;
select 
a.processing_date processing_date,
a.datecode datecode,
b.processing_date processing_date2,
a.axe_revenu axe_revenu,
a.axe_subscriber axe_subscriber, 
a.valeur valeur,
b.valeur valeur2 
from 
(select * from 

(select 
    processing_date,
    axe_revenu,
    axe_subscriber,
    sum(valeur) valeur 
from MON.SPARK_KPIS_REG_FINAL 
group by processing_date ,axe_revenu,    axe_subscriber
order by 1 
) a
left join (
    select datecode from dim.dt_dates 
) b on a.processing_date between datecode-6 and datecode
) a
left join (
    select 
    processing_date,
    axe_revenu,
    axe_subscriber,
    sum(valeur) valeur 
from MON.SPARK_KPIS_REG_FINAL 
group by processing_date ,axe_revenu,    axe_subscriber
order by 1 
)b on a.datecode=b.processing_date and nvl(a.axe_revenu,'nd') =nvl(b.axe_revenu,'nd') and nvl(a.axe_subscriber,'nd')=nvl(b.axe_subscriber,'nd')
