
SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0  AND LAST_SITE>0 and SITE_TRAFFIC>0 and zebra_master_balance>0 and retail>0 and subs>0 AND T2.INSERT_COUNT=1 AND T3.INSERT_COUNT=2 AND T4.INSERT_COUNT=1 AND T6.INSERT_COUNT=3 AND T7.INSERT_COUNT=1 AND 
ABS((refill_amount_valeur_j_0 / refill_amount_valeur_j_1) - 1) < 0.2 and 
ABS((rated_amount_valeur_j_0 / rated_amount_valeur_j_1) - 1) < 0.2 and 
ABS((available_balance_valeur_j_0 / available_balance_valeur_j_1) - 1) < 0.2 
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_REFILL_TRAFFIC')T1,
(SELECT COUNT(*) FT_EXISTS, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.SPARK_FT_REFILL WHERE FILE_DATE = '###SLICE_VALUE###' )T2,
(SELECT COUNT(*) LAST_SITE, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) SITE_TRAFFIC, COUNT(distinct REFRESH_DATE) INSERT_COUNT FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T4,
(SELECT COUNT(*) zebra_master_balance, SUM(available_balance) available_balance_valeur_j_0 FROM cdr.spark_IT_ZEBRA_MASTER_BALANCE WHERE EVENT_DATE = '###SLICE_VALUE###')T5,
(SELECT COUNT(*) retail, COUNT(distinct INSERT_DATE) INSERT_COUNT, SUM(refill_amount) refill_amount_valeur_j_0 FROM MON.SPARK_FT_RETAIL_BASE_DETAILLANT WHERE refill_date = '###SLICE_VALUE###')T6,
(SELECT COUNT(*) subs, COUNT(distinct INSERT_DATE) INSERT_COUNT, SUM(rated_amount) rated_amount_valeur_j_0 FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###')T7,
(SELECT SUM(refill_amount) refill_amount_valeur_j_1 FROM MON.SPARK_FT_RETAIL_BASE_DETAILLANT WHERE refill_date = date_sub('###SLICE_VALUE###', 1))T8,
(SELECT SUM(rated_amount) rated_amount_valeur_j_1 FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE = date_sub('###SLICE_VALUE###', 1))T9,
(SELECT SUM(available_balance) available_balance_valeur_j_1 FROM cdr.spark_IT_ZEBRA_MASTER_BALANCE WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1))T10