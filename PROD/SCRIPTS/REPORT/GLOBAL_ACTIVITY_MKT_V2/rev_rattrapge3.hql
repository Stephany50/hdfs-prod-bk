
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
    sum(valeur_day) valeur
from (
select 
    region_administrative,
    region_commerciale,
    category,
    kpi,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    source_table,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur else valeur end) valeur,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_day else valeur_day end) valeur_day,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_lweek else valeur_lweek end) valeur_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then v2wa else v2wa end) v2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then v3wa else v3wa end) v3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then v4wa else v4wa end) v4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_mtd else valeur_mtd end) valeur_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_lmtd else valeur_lmtd end) valeur_lmtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget else budget end) budget,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_lweek else budget_lweek end) budget_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_2wa else budget_2wa end) budget_2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_3wa else budget_3wa end) budget_3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_4wa else budget_4wa end) budget_4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_budget_mtd else valeur_budget_mtd end) valeur_budget_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_mtd_last_year else valeur_mtd_last_year end) valeur_mtd_last_year,
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
 from mon.SPARK_KPIS_REG_FINAL) where GRANULARITE_REG='NATIONAL'
group by processing_date ,axe_revenu,    axe_subscriber
order by 1
) a
left join (
    select datecode from dim.dt_dates
) b on a.processing_date between datecode and datecode +6
) a
left join (
    select
    processing_date,
    axe_revenu,
    axe_subscriber,
    sum(valeur_day) valeur
from (
select 
    region_administrative,
    region_commerciale,
    category,
    kpi,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    source_table,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur else valeur end) valeur,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_day else valeur_day end) valeur_day,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_lweek else valeur_lweek end) valeur_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then v2wa else v2wa end) v2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then v3wa else v3wa end) v3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then v4wa else v4wa end) v4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_mtd else valeur_mtd end) valeur_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_lmtd else valeur_lmtd end) valeur_lmtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget else budget end) budget,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_lweek else budget_lweek end) budget_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_2wa else budget_2wa end) budget_2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_3wa else budget_3wa end) budget_3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then budget_4wa else budget_4wa end) budget_4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_budget_mtd else valeur_budget_mtd end) valeur_budget_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM','RECHARGE') then valeur_mtd_last_year else valeur_mtd_last_year end) valeur_mtd_last_year,
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
 from mon.SPARK_KPIS_REG_FINAL)  where GRANULARITE_REG='NATIONAL'
group by processing_date ,axe_revenu,    axe_subscriber
order by 1
)b on a.datecode=b.processing_date and nvl(a.axe_revenu,'nd') =nvl(b.axe_revenu,'nd') and nvl(a.axe_subscriber,'nd')=nvl(b.axe_subscriber,'nd')