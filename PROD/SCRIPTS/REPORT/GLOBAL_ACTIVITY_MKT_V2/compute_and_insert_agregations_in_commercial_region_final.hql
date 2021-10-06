INSERT INTO AGG.SPARK_KPIS_DG_FINAL
SELECT
    CONCAT_WS(',',COLLECT_LIST(region_administrative)) region_administrative,
    region_commerciale,
    category,
    kpi,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    source_table,
    SUM(valeur) valeur,
    SUM(valeur_day) valeur_day,
    sum(lweek) lweek,
    sum(v2wa) v2wa,
    sum(v3wa) v3wa,
    sum(v4wa) v4wa,
    sum(mtd) mtd,
    sum(lmtd) lmtd,
    sum(budget) budget,
    sum(budget_lweek) budget_lweek,
    sum(budget_2wa) budget_2wa,
    sum(budget_3wa) budget_3wa,
    sum(budget_4wa) budget_4wa,
    sum(budget_mtd) budget_mtd,
    sum(mtd_last_year) mtd_last_year,
    if(sum(nvl(lweek,0)) is null ,null,round(((sum(valeur)-sum(nvl(lweek,0)))/sum(nvl(lweek,0)))*100,2) ) vslweek,
    if(sum(nvl(v2wa,0)) is null ,null,round(((sum(valeur)-sum(nvl(v2wa,0)))/sum(nvl(v2wa,0)))*100,2))   vs2wa,
    if(sum(nvl(v3wa,0)) is null ,null,round(((sum(valeur)-sum(nvl(v3wa,0)))/sum(nvl(v3wa,0)))*100,2))   vs3wa,
    if(sum(nvl(v4wa,0)) is null ,null,round(((sum(valeur)-sum(nvl(v4wa,0)))/sum(nvl(v4wa,0)))*100,2))  vs4wa,
    if(sum(nvl(lmtd,0)) is null ,null,round(((sum(nvl(mtd,0))-sum(nvl(lmtd,0)))/sum(nvl(lmtd,0)))*100,2))   mtdvslmdt,
    if(sum(nvl(budget_mtd,0)) is null ,null,round(((sum(nvl(mtd,0))-sum(nvl(budget_mtd,0)))/sum(nvl(budget_mtd,0)))*100,2)) mdtvsbudget,
    if(sum(nvl(budget,0)) is null ,null,round(((sum(valeur)-sum(nvl(budget,0)))/sum(nvl(budget,0)))*100,2)) weekvsbudget,
    if(sum(nvl(budget_lweek,0)) is null ,null,round(((sum(nvl(lweek,0))-sum(nvl(budget_lweek,0)))/sum(nvl(budget_lweek,0)))*100,2)) lweekvsblweek,
    if(sum(nvl(budget_2wa,0)) is null ,null,round(((sum(nvl(v2wa,0))-sum(nvl(budget_2wa,0)))/sum(nvl(budget_2wa,0)))*100,2)) v2wavsb2wa,
    if(sum(nvl(budget_3wa,0)) is null ,null,round(((sum(nvl(v3wa,0))-sum(nvl(budget_3wa,0)))/sum(nvl(budget_3wa,0)))*100,2)) v3wavsb3wa,
    if(sum(nvl(budget_4wa,0)) is null ,null,round(((sum(nvl(v4wa,0))-sum(nvl(budget_4wa,0)))/sum(nvl(budget_4wa,0)))*100,2)) v4wavsb4wa,
    if(sum(nvl(mtd_last_year,0)) is null ,null,round(((sum(nvl(mtd,0))-sum(nvl(mtd_last_year,0)))/sum(nvl(mtd_last_year,0)))*100,2))   mtd_vs_last_year,
    'COMMERCIAL_REGION' granularite_reg,
    CURRENT_TIMESTAMP insert_date,
    processing_date
FROM AGG.SPARK_KPIS_DG_FINAL where processing_date ='###SLICE_VALUE###' and granularite_reg='ADMINISTRATIVE_REGION'
group by
region_commerciale,
category,
kpi,
axe_vue_transversale,
axe_revenu,
axe_subscriber,
source_table,
processing_date

