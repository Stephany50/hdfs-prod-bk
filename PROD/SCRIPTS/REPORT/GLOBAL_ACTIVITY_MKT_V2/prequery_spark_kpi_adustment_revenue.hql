SELECT IF(T1.KPI_IS_LOAD=0 AND T2.CONTRACT_EXISTS>0 AND LAST_SITE>0 and SITE_TRAFFIC>0 AND T2.INSERT_COUNT=8 AND T3.INSERT_COUNT=2 AND T4.INSERT_COUNT=1,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_ADJUSTMENT')T1,
(SELECT COUNT(*) CONTRACT_EXISTS, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',7) AND '###SLICE_VALUE###')T2,
(SELECT COUNT(*) LAST_SITE, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) SITE_TRAFFIC, COUNT(distinct REFRESH_DATE) INSERT_COUNT FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T4