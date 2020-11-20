INSERT INTO AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    'MONTHLY' granularite,
    CASE 
        WHEN cummulable ='MOY' THEN AVG(valeur) 
        WHEN cummulable ='SUM' THEN SUM(valeur) 
        ELSE SUM(valeur)
    END valeur,
    cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' processing_date,
    source_table
from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'  and  granularite='DAILY'  and cummulable <>'MAX'
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    cummulable,
    source_table
    
union all 
SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    'MONTHLY' granularite,
    SUM(valeur) valeur,
    cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' processing_date,
    source_table
from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date = '###SLICE_VALUE###'   and  granularite='DAILY'  and cummulable in ('MAX','MONTHLY')
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    cummulable,
    source_table
    