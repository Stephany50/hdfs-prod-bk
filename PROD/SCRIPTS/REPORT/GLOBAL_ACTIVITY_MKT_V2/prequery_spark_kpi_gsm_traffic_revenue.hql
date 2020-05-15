SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V2  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY')T1,
(SELECT COUNT(*) FT_EXISTS FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###')T2
