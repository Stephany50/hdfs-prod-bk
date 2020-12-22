
SELECT IF(T1.KPI_IS_LOAD=0 AND DM_EXISTS AND FT_EXISTS>0 ,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_A_DATAMART_OM_MARKETING  WHERE JOUR='###SLICE_VALUE###')T1,
(SELECT COUNT(*) DM_EXISTS FROM MON.SPARK_DATAMART_OM_MARKETING2 WHERE JOUR = '###SLICE_VALUE###' )T2,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' )T3