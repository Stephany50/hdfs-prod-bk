
SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0  AND LAST_SITE>0 and SITE_TRAFFIC>0 AND T2.INSERT_COUNT=1 AND T3.INSERT_COUNT=2 AND T4.INSERT_COUNT=1 AND 
ABS((fee_valeur_j_0 / fee_valeur_j_1) - 1) < 0.5
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_OVERDRAFT_REVENUE')T1,
(SELECT COUNT(*) FT_EXISTS, COUNT(distinct INSERT_DATE) INSERT_COUNT, SUM(FEE) fee_valeur_j_0 FROM MON.SPARK_FT_OVERDRAFT WHERE TRANSACTION_DATE = '###SLICE_VALUE###' )T2,
(SELECT COUNT(*) LAST_SITE, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) SITE_TRAFFIC, COUNT(distinct REFRESH_DATE) INSERT_COUNT FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T4,
(SELECT SUM(FEE) fee_valeur_j_1 FROM MON.SPARK_FT_OVERDRAFT WHERE TRANSACTION_DATE = date_sub('###SLICE_VALUE###', 1))T5