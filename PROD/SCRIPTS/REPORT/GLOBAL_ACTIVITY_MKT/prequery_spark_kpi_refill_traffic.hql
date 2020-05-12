SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_REFILL_SLICE_MKT  WHERE TRANSACTION_DATE='###SLICE_VALUE###' )T1,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_REFILL WHERE REFILL_DATE='###SLICE_VALUE###')T2