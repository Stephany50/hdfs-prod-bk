SELECT IF(T1.KPI_IS_LOAD=0 AND bal>0 and LAST_SITE>0 and SITE_TRAFFIC>0 AND T3.INSERT_COUNT=2 AND T4.INSERT_COUNT=1 -- AND 
-- ABS((balance_valeur_j_0 / balance_valeur_j_1) - 1) < 0.5 
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_OM_BALANCE')T1,
(SELECT COUNT(*) bal FROM cdr.SPARK_IT_OM_ALL_BALANCE WHERE original_file_date ='###SLICE_VALUE###')T2,
(SELECT COUNT(*) LAST_SITE, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) SITE_TRAFFIC, COUNT(distinct REFRESH_DATE) INSERT_COUNT FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T4,
(select sum(nvl(balance,0)) balance_valeur_j_0 from (select * from CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE ='###SLICE_VALUE###' AND USER_CATEGORY IN ('Subscriber')) T) T5,
(select sum(nvl(balance,0)) balance_valeur_j_1 from (select * from CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE =date_sub('###SLICE_VALUE###', 1) AND USER_CATEGORY IN ('Subscriber')) T) T6