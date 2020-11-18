INSERT INTO TMP.SPARK_KPIS_REG5

------- Revenue overview  Telco (prepayé+hybrid) + OM
SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    'MONTHLY' granularite,
    CASE 
        WHEN cummulable ='MOY' THEN AVG(valeur) 
        WHEN cummulable ='SUM' THEN SUM(valeur) 
        ELSE SUM(valeur)
    END valeur,
    cummulable,
    '###SLICE_VALUE###' processing_date
from TMP.SPARK_KPIS_REG5 where processing_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'  and  granularite='DAILY'  and cummulable <>'MAX'
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    cummulable
    
union all 
SELECT
    region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    'MONTHLY' granularite,
    SUM(valeur) valeur,
    cummulable,
    '###SLICE_VALUE###' processing_date
from TMP.SPARK_KPIS_REG5 where processing_date = '###SLICE_VALUE###'   and  granularite='DAILY'  and cummulable in ('MAX','MONTHLY')
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    cummulable
    