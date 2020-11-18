SELECT IF(T1.KPI_IS_LOAD=0 AND T2.CONTRACT_EXISTS>0 AND T5.OM_MARK>0 AND T4.OM_DIST>0 AND LAST_SITE>0 and SITE_TRAFFIC>0 and BAL>0,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND KPI IN ('REVENUE_OM'))T1,
(SELECT COUNT(*) CONTRACT_EXISTS FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',7) AND '###SLICE_VALUE###')T2,
(SELECT COUNT(*) LAST_SITE FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) OM_DIST FROM MON.SPARK_DATAMART_OM_DISTRIB WHERE jour ='###SLICE_VALUE###')T4,
(SELECT COUNT(*) OM_MARK FROM MON.SPARK_DATAMART_OM_MARKETING2 WHERE jour ='###SLICE_VALUE###')T5,
(SELECT COUNT(*) SITE_TRAFFIC FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T6,
(SELECT COUNT(*) BAL FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE original_file_date = '###SLICE_VALUE###')T7