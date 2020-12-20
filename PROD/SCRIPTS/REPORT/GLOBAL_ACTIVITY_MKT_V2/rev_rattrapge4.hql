select if(is_ok=0 and monthly>0 and weekly>0 and (case when '2020-09-04' >='2020-11-15' then nb_dates else nb_dates+1 end ) >=6,'OK','NOK')
from
(select count(*) is_ok from AGG.SPARK_KPIS_DG_FINAL where processing_date='2020-09-04'  )a,
(select COUNT(*) weekly from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date='2020-09-04' AND granularite='WEEKLY')b,
(select COUNT(*) monthly from AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where processing_date='2020-09-04' AND granularite='MONTHLY')c,
(select count(distinct processing_date) nb_dates from  AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE where (processing_date in (date_sub('2020-09-04',6) , date_sub('2020-09-04',14),date_sub('2020-09-04',21),date_sub('2020-09-04',28)) and  granularite='WEEKLY' ) or (processing_date=add_months('2020-09-04',-1) and granularite='MONTHLY') or (processing_date=add_months('2020-09-04',-12) and granularite='MONTHLY'))d
