SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0 AND nbs_ci>1000 AND T2.INSERT_COUNT=1 AND 
ABS((main_rated_amount_valeur_j_0 / main_rated_amount_valeur_j_1) - 1) < 0.2 AND 
ABS((promo_rated_valeur_j_0 / promo_rated_valeur_j_1) - 1) < 0.2 AND 
ABS((rated_count_or_duration_valeur_j_0 / rated_count_or_duration_valeur_j_1) - 1) < 0.2 
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_GSM_TRAFFIC')T1,
(SELECT COUNT(*) FT_EXISTS,count(distinct location_ci ) nbs_ci, COUNT(distinct INSERT_DATE) INSERT_COUNT, 
SUM(MAIN_RATED_AMOUNT) main_rated_amount_valeur_j_0, SUM(PROMO_RATED_AMOUNT) promo_rated_valeur_j_0,
SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN RATED_TOTAL_COUNT
        WHEN SERVICE_CODE='VOI_VOX' THEN RATED_DURATION ELSE 0 END) rated_count_or_duration_valeur_j_0
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###')T2,
(
    SELECT 
        SUM(MAIN_RATED_AMOUNT) main_rated_amount_valeur_j_1, 
        SUM(PROMO_RATED_AMOUNT) promo_rated_valeur_j_1,
        SUM(
            CASE 
                WHEN SERVICE_CODE='NVX_SMS' THEN RATED_TOTAL_COUNT
                WHEN SERVICE_CODE='VOI_VOX' THEN RATED_DURATION 
                ELSE 0 
            END
        ) rated_count_or_duration_valeur_j_1
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###')T3