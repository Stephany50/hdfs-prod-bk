INSERT INTO AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE

SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    'WEEKLY' granularite,
    SUM(valeur)  valeur,
    cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' processing_date,
    max(source_table) source_table
from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###' and  granularite='DAILY' and cummulable ='SUM'
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    cummulable
union all
SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    'WEEKLY' granularite,
    AVG(valeur)  valeur,
    cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' processing_date,
    max(source_table) source_table
from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'    and  granularite='DAILY'  and cummulable ='MOY'
group by
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    cummulable
union all
SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    'WEEKLY' granularite,
    SUM(valeur) valeur,
    cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' processing_date,
    max(source_table) source_table
from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date = '###SLICE_VALUE###'  and  granularite='DAILY'   and cummulable in ('MAX','WEEKLY')
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    cummulable