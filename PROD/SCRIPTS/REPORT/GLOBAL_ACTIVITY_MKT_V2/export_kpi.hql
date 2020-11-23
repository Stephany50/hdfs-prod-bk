
 insert into mon.kpi_reg_final3 select
 region_administrative,
 region_commerciale,
 category,
 kpi,
 axe_revenu,
 axe_subscriber,
 axe_vue_transversale,
 valeur,
 lweek,
 2wa wa2,
 3wa wa3,
 4wa wa4,
 mtd,
 lmtd,
 budget,
 budget_mtd,
 mtd_last_year,
 vslweek,
 vs2wa,
 vs3wa,
 vs4wa,
 mtdvslmdt,
 mdtvsbudget,
 mtd_vs_last_year,
 processing_date
 from tmp.kpi_reg_final

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
