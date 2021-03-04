
SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0  AND LAST_SITE>0 and SITE_TRAFFIC>0 and conso_msisdn>0 AND nbs_ci>1000 AND T2.INSERT_COUNT=1 AND T3.INSERT_COUNT=2 AND T4.INSERT_COUNT=1 AND T5.INSERT_COUNT=1 AND 
ABS((subs_amount_valeur_j_0 / subs_amount_valeur_j_1) -1) < 0.2 AND 
ABS((amount_data_valeur_j_0 / amount_data_valeur_j_1) -1) < 0.2 AND 
ABS((bundle_tel_duration_valeur_j_0 / bundle_tel_duration_valeur_j_1) -1) < 0.2 AND 
ABS((bundle_sms_count_valeur_j_0 / bundle_sms_count_valeur_j_1) -1) < 0.2
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_SUBSCRIPTION_REVENUE')T1,
(SELECT COUNT(*) FT_EXISTS ,count(distinct location_ci ) nbs_ci, COUNT(distinct INSERT_DATE) INSERT_COUNT, sum(SUBS_AMOUNT) subs_amount_valeur_j_0, sum(AMOUNT_DATA) amount_data_valeur_j_0 FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###' )T2,
(SELECT COUNT(*) LAST_SITE, COUNT(distinct INSERT_DATE) INSERT_COUNT FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='###SLICE_VALUE###')T3,
(SELECT COUNT(*) SITE_TRAFFIC, COUNT(distinct REFRESH_DATE) INSERT_COUNT FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '###SLICE_VALUE###')T4,
(SELECT COUNT(*) conso_msisdn, COUNT(distinct INSERT_DATE) INSERT_COUNT, sum(BUNDLE_TEL_DURATION) bundle_tel_duration_valeur_j_0, sum(BUNDLE_SMS_COUNT) bundle_sms_count_valeur_j_0 FROM MON.SPARK_FT_CONSO_MSISDN_DAY WHERE EVENT_DATE = '###SLICE_VALUE###')T5,
(SELECT sum(SUBS_AMOUNT) subs_amount_valeur_j_1, sum(AMOUNT_DATA) amount_data_valeur_j_1 FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE = date_sub('###SLICE_VALUE###', 1))T6,
(SELECT sum(BUNDLE_TEL_DURATION) bundle_tel_duration_valeur_j_1, sum(BUNDLE_SMS_COUNT) bundle_sms_count_valeur_j_1 FROM MON.SPARK_FT_CONSO_MSISDN_DAY WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1))T7
