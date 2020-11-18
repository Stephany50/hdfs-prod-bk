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
    'WEEKLY' granularite,
    SUM(valeur)  valeur,
    cummulable,
    '###SLICE_VALUE###' processing_date
from TMP.SPARK_KPIS_REG5 where processing_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###' and  granularite='DAILY' and cummulable ='SUM'
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
    'WEEKLY' granularite,
    AVG(valeur)  valeur,
    cummulable,
    '###SLICE_VALUE###' processing_date
from TMP.SPARK_KPIS_REG5 where processing_date between date_sub('###SLICE_VALUE###',6) and  '###SLICE_VALUE###'    and  granularite='DAILY'  and cummulable ='MOY'
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
    'WEEKLY' granularite,
    SUM(valeur) valeur,
    cummulable,
    '###SLICE_VALUE###' processing_date
from TMP.SPARK_KPIS_REG5 where processing_date = '###SLICE_VALUE###'  and  granularite='DAILY'   and cummulable in ('MAX','WEEKLY')
group by 
  region_administrative,
    region_commerciale,
    category,
    KPI,
    axe_revenue,
    axe_subscriber,
    axe_regionale,
    cummulable
    