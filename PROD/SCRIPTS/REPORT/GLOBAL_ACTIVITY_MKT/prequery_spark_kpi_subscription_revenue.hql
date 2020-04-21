SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_A_SUBSCRIPTION>0 AND T3.FT_CONSO_MSISDN_DAY>0,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND SOURCE_TABLE in('FT_A_SUBSCRIPTION','FT_CONSO_MSISDN_DAY'))T1,
(SELECT COUNT(*) FT_A_SUBSCRIPTION FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###')T2,
(SELECT COUNT(*) FT_CONSO_MSISDN_DAY FROM MON.SPARK_FT_CONSO_MSISDN_DAY WHERE EVENT_DATE='###SLICE_VALUE###')T3