SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0 AND nbs_ci>1000 AND T2.INSERT_COUNT=1 AND 
ABS((main_cost_valeur_j_0 / main_cost_valeur_j_1) -1) < 0.2 AND 
ABS((promo_cost_valeur_j_0 / promo_cost_valeur_j_1) -1) < 0.2 AND 
ABS((billed_unit_valeur_j_0 / billed_unit_valeur_j_1) -1) < 0.2 AND 
ABS((bucket_value_valeur_j_1 / bucket_value_valeur_j_1) -1) < 0.2 AND 
ABS((bytes_valeur_j_0 / bytes_valeur_j_1) -1) < 0.2
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_GPRS')T1,
(SELECT COUNT(*) FT_EXISTS ,count(distinct location_ci ) nbs_ci, COUNT(distinct INSERT_DATE) INSERT_COUNT, 
SUM(MAIN_COST) main_cost_valeur_j_0, SUM(PROMO_COST) promo_cost_valeur_j_0, SUM(BILLED_UNIT) billed_unit_valeur_j_0, SUM(BUCKET_VALUE) bucket_value_valeur_j_0,
SUM(BYTES_RECV+BYTES_SEND) bytes_valeur_j_0 
FROM AGG.SPARK_FT_A_GPRS_ACTIVITY WHERE DATECODE='###SLICE_VALUE###')T2,
(
    SELECT 
        SUM(MAIN_COST) main_cost_valeur_j_1, 
        SUM(PROMO_COST) promo_cost_valeur_j_1, 
        SUM(BILLED_UNIT) billed_unit_valeur_j_1, 
        SUM(BUCKET_VALUE) bucket_value_valeur_j_1,
        SUM(BYTES_RECV+BYTES_SEND) bytes_valeur_j_1 
    FROM AGG.SPARK_FT_A_GPRS_ACTIVITY WHERE DATECODE=date_sub('###SLICE_VALUE###', 1)
)T3

