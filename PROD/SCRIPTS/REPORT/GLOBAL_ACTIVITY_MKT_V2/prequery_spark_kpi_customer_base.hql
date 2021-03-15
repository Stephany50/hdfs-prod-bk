SELECT
    IF(T1.KPI_IS_LOAD=0 AND
    T2.FT_GROUP_USER_BASE>0 AND
    T3.FT_GROUP_SUBSCRIBER_SUMMARY >0 AND
    T4.FT_A_SUBSCRIBER_SUMMARY>0 AND
    T5.FT_COMMERCIAL_SUBSCRIB_SUMMARY>0 AND
    T5.nbs_ci>1000 AND
    T5.nbs_ci>1000 AND
    T2.insert_count=1 AND
    T3.nbs_ci>1000 AND
    T4.nbs_ci>1000 AND
    T2.INSERT_COUNT=1 AND
    T3.INSERT_COUNT=1 AND
    T4.INSERT_COUNT=1 AND
    T6.SPARK_FT_SUBSCRIPTION>0 and
    T6.INSERT_COUNT=1 and 
    ABS((served_party_msisdn_valeur_j_0 / served_party_msisdn_valeur_j_1) - 1) <= 0.4 and 
    ABS((effectif_valeur_j_0 / effectif_valeur_j_1) - 1) <= 0.4 and 
    ABS((total_count_valeur_j_0 / total_count_valeur_j_1) - 1) <= 0.4 
    ,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME ='COMPUTE_KPI_CUSTOMER_BASE')T1,
(SELECT COUNT(*) FT_GROUP_USER_BASE,count(distinct location_ci ) nbs_ci, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.SPARK_FT_GROUP_USER_BASE WHERE EVENT_DATE='###SLICE_VALUE###')T2,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY,count(distinct location_ci ) nbs_ci, COUNT(distinct INSERT_DATE) INSERT_COUNT, SUM(EFFECTIF) effectif_valeur_j_0 FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE= DATE_ADD('###SLICE_VALUE###',1))T3,
(SELECT COUNT(*) FT_A_SUBSCRIBER_SUMMARY,count(distinct location_ci ) nbs_ci, COUNT(distinct REFRESH_DATE) INSERT_COUNT, SUM(TOTAL_ACTIVATION) total_activation_valeur_j_0 FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE DATECODE='###SLICE_VALUE###')T4,
(SELECT COUNT(*) FT_COMMERCIAL_SUBSCRIB_SUMMARY,count(distinct location_ci ) nbs_ci, COUNT(distinct REFRESH_DATE) INSERT_COUNT, SUM(TOTAL_COUNT) total_count_valeur_j_0 FROM mon.spark_FT_COMMERCIAL_SUBSCRIB_SUMMARY WHERE DATECODE='###SLICE_VALUE###')T5,
(SELECT COUNT(*) SPARK_FT_SUBSCRIPTION,COUNT(distinct INSERT_date) INSERT_COUNT, SUM(SERVED_PARTY_MSISDN) served_party_msisdn_valeur_j_0 FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###')T6,
(SELECT SUM(SERVED_PARTY_MSISDN) served_party_msisdn_valeur_j_1 FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE=date_sub('###SLICE_VALUE###', 1))T7,
(SELECT SUM(EFFECTIF) effectif_valeur_j_1 FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE= '###SLICE_VALUE###')T8,
(SELECT SUM(TOTAL_COUNT) total_count_valeur_j_1 FROM mon.spark_FT_COMMERCIAL_SUBSCRIB_SUMMARY WHERE DATECODE=date_sub('###SLICE_VALUE###', 1))T9,
(SELECT SUM(TOTAL_ACTIVATION) total_activation_valeur_j_1 FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE DATECODE=date_sub('###SLICE_VALUE###', 1))T10