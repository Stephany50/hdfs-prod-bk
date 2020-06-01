
SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0  AND LAST_SITE>0 and SITE_TRAFFIC>0,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V2  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND SOURCE_TABLE='FT_A_SUBSCRIPTION')T1,
(SELECT COUNT(*) FT_EXISTS FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###' )T2,
(SELECT COUNT(*) LAST_SITE FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) SITE_TRAFFIC FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T4

