SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0 AND nbs_ci>1000,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_CONTRACT_SNAPSHOT')T1,
(SELECT COUNT(*) FT_EXISTS ,count(distinct location_ci ) nbs_ci FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###')T2
