
SELECT
BASE_MONTH ,
CASE TOTAL_USERS_BUNDLE_DATA WHEN -1 THEN 0 ELSE TOTAL_USERS_BUNDLE_DATA END TOTAL_USERS_BUNDLE_DATA,
MAIN_COST_BUNDLE_DATA,
CASE TOTAL_USERS_PAYASGO_DATA WHEN -1 THEN 0 ELSE TOTAL_USERS_PAYASGO_DATA END TOTAL_USERS_PAYASGO_DATA,
MAIN_COST_PAYASGO_DATA,
TOTAL_USERS_DATA,
main_cost_data

FROM
(
SELECT base_month,
count(DISTINCT (CASE WHEN  main_cost_subscr_forfait_data > 0 THEN MSISDN ELSE '' END))-1 total_users_bundle_data ,
sum(main_cost_subscr_forfait_data) main_cost_bundle_data,
count(DISTINCT (CASE WHEN main_cost_data > 0 THEN MSISDN ELSE '' END))-1 total_users_payasgo_data,
sum(main_cost_data) main_cost_payasgo_data,
count(DISTINCT MSISDN) total_users_data,
sum(main_cost_subscr_forfait_data + main_cost_data) main_cost_data
FROM MON.SPARK_TF_BASE_GROUP_CONSO_MONTH
WHERE base_month = substr('###SLICE_VALUE###',1,7)
AND (main_cost_data > 0 OR main_cost_subscr_forfait_data > 0)
GROUP BY base_month
) T