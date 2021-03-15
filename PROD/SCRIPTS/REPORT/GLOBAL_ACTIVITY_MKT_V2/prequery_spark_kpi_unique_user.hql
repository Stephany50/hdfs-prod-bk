
SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_USERS_DAY>0 AND T3.FT_USERS_DATA_DAY>0 AND T2.nbs_ci>1000 AND T3.nbs_ci>1000 AND T2.INSERT_COUNT=1 AND T3.INSERT_COUNT=1 AND 
ABS((rated_count_valeur_j_0 / rated_count_valeur_j_1) - 1) <= 0.4 
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_UNIQUE_USER')T1,
(SELECT COUNT(*) FT_USERS_DAY,count(distinct location_ci ) nbs_ci, COUNT(DISTINCT INSERT_DATE) INSERT_COUNT FROM MON.SPARK_FT_USERS_DAY WHERE EVENT_DATE='###SLICE_VALUE###')T2,
(SELECT COUNT(*) FT_USERS_DATA_DAY,count(distinct location_ci ) nbs_ci, COUNT(DISTINCT INSERT_DATE) INSERT_COUNT, SUM(rated_count) rated_count_valeur_j_0 
FROM MON.SPARK_FT_USERS_DATA_DAY WHERE EVENT_DATE='###SLICE_VALUE###')T3,
(SELECT SUM(rated_count) rated_count_valeur_j_1 FROM MON.SPARK_FT_USERS_DATA_DAY WHERE EVENT_DATE=date_sub('###SLICE_VALUE###', 1))T4
