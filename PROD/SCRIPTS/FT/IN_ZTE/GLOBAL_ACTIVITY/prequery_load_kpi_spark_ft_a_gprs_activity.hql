SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0,'OK','NOK')
FROM
    (SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND SOURCE_DATA='FT_A_GPRS_ACTIVITY')T1,
    (SELECT COUNT(*) FT_EXISTS FROM AGG.SPARK_FT_A_GPRS_ACTIVITY WHERE DATECODE='###SLICE_VALUE###')T2