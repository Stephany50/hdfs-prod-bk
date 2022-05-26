SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0 AND nbs_ci>1000 AND T2.INSERT_COUNT=1,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_CONTRACT_SNAPSHOT')T1,
(SELECT COUNT(*) FT_EXISTS ,count(distinct location_ci ) nbs_ci, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###')T2
