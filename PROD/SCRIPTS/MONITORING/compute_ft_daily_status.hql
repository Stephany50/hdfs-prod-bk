
INSERT INTO MON.FT_DAILY_STATUS
select  TABLE_TYPE, TABLE_SOURCE, TABLE_NAME, '' DEPENDANCES, IF(NB_ROWS < 1, 'NOK', 'OK') STATUT, NB_ROWS, null LAST_INCIDENT_DAY, TABLE_INSERT_DATE, current_timestamp INSERT_DATE,'###SLICE_VALUE###' TABLE_DATE
from (
SELECT 'FT' table_type, 'IN' table_source, 'FT_BILLED_TRANSACTION_PREPAID' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_BILLED_TRANSACTION_PREPAID where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_BILLED_TRANSACTION_POSTPAID' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_BILLED_TRANSACTION_POSTPAID where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_CRA_GPRS' table_name, count(*) nb_rows, max(DWH_FT_ENTRY_DATE) table_insert_date  from MON.FT_CRA_GPRS where session_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_CRA_GPRS_POST' table_name, count(*) nb_rows, max(DWH_FT_ENTRY_DATE) table_insert_date  from MON.FT_CRA_GPRS_POST where session_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_RECHARGE' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_RECHARGE where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_SUBSCRIPTION' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.SPARK_FT_SUBSCRIPTION where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_DATA_TRANSFER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_DATA_TRANSFER where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_EMERGENCY_DATA' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_EMERGENCY_DATA where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_OVERDRAFT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_OVERDRAFT where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_CONTRACT_SNAPSHOT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  FROM MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_CREDIT_TRANSFER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_CREDIT_TRANSFER where refill_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_EMERGENCY_CREDIT_ACTIVITY' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_EMERGENCY_CREDIT_ACTIVITY where EVENT_DATE = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_DATA_TRANSFER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_A_DATA_TRANSFER where event_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_GPRS_ACTIVITY' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_A_GPRS_ACTIVITY where datecode = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_GPRS_ACTIVITY_POST' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_A_GPRS_ACTIVITY_POST where datecode = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_SUBSCRIPTION' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_A_SUBSCRIPTION where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_GSM_TRAFFIC_REVENUE_DAILY' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_GSM_TRAFFIC_REVENUE_DAILY where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_GSM_TRAFFIC_REVENUE_POST' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_GSM_TRAFFIC_REVENUE_POST where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_EMERGENCY_DATA' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_A_EMERGENCY_DATA where EVENT_DATE = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'MSC' table_source, 'FT_MSC_TRANSACTION' table_name, count(*) nb_rows, max(ft_insert_date) table_insert_date  from MON.FT_MSC_TRANSACTION where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'MSC' table_source, 'FT_AG_INTERCO' table_name, count(*) nb_rows, max(INSERTED_DATE) table_insert_date  from AGG.FT_AG_INTERCO where sdate = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'MSC' table_source, 'FT_X_INTERCO_FINAL' table_name, count(*) nb_rows, max(INSERTED_DATE) table_insert_date  from AGG.FT_X_INTERCO_FINAL where sdate = '###SLICE_VALUE###'

union
SELECT 'FT' table_type, 'MVAS' table_source, 'FT_SMSC_TRANSACTION_A2P' table_name, count(*) nb_rows, max(ft_insert_date) table_insert_date  from MON.FT_SMSC_TRANSACTION_A2P where transaction_billing_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'MVAS' table_source, 'FT_QOS_SMSC_SPECIAL_NUMBER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_QOS_SMSC_SPECIAL_NUMBER where state_date = '###SLICE_VALUE###'

union
SELECT 'FT' table_type, 'ZEBRA' table_source, 'FT_REFILL' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_REFILL where refill_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'ZEBRA' table_source, 'FT_SUBS_RETAIL_ZEBRA' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_SUBS_RETAIL_ZEBRA where transaction_date = '###SLICE_VALUE###'

union
SELECT 'FT' table_type, 'OTARIE' table_source, 'FT_OTARIE_DATA_TRAFFIC_DAY' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_OTARIE_DATA_TRAFFIC_DAY where transaction_date = '###SLICE_VALUE###'

union
SELECT 'FT' table_type, 'EQUATION_PREPAYEE' table_source, 'FT_PRPD_EQT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_PRPD_EQT where event_day = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'EQUATION_PREPAYEE' table_source, 'FT_EDR_PRPD_EQT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_EDR_PRPD_EQT where event_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'EQUATION_PREPAYEE' table_source, 'FT_A_EDR_PRPD_EQT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from AGG.FT_A_EDR_PRPD_EQT where event_day = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'REPORT' table_source, 'GLOBAL_ACTIVITY' table_name, if(count(distinct source_data) <13,0,count(distinct source_data))  nb_rows, max(insert_date) table_insert_date  from AGG.FT_GLOBAL_ACTIVITY_DAILY where TRANSACTION_DATE = '###SLICE_VALUE###'
union
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_ACCOUNT_ACTIVITY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_CBM_BUNDLE_SUBS_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_CBM_BUNDLE_SUBS_DAILY WHERE PERIOD='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_CBM_CHURN_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_CBM_CHURN_DAILY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_CBM_CUST_INSIGTH_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_CBM_CUST_INSIGTH_DAILY WHERE PERIOD='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_CLIENT_LAST_SITE_DAY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_CLIENT_SITE_TRAFFIC_DAY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(refresh_date) TABLE_INSERT_DATE  FROM MON.FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_COMMERCIAL_SUBSCRIB_SUMMARY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(refresh_date) TABLE_INSERT_DATE  FROM MON.SPARK_FT_COMMERCIAL_SUBSCRIB_SUMMARY WHERE DATECODE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_DATA_DA_USAGE_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_DATA_DA_USAGE_DAILY WHERE SESSION_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_CBM_DA_USAGE_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_CBM_DA_USAGE_DAILY WHERE PERIOD='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_VOICE_SMS_DA_USAGE_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_VOICE_SMS_DA_USAGE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_GROUP_SUBSCRIBER_SUMMARY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_LAST_UPDATE_EC_EXTRACT' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_LAST_UPDATE_EC_EXTRACT WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_OG_IC_CALL_SNAPSHOT' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OG_IC_CALL_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_OTHER_VAS' TABLE_NAME, COUNT(*) NB_ROWS, MAX(dwh_entry_date) TABLE_INSERT_DATE  FROM MON.FT_OTHER_VAS WHERE TRANSACTION_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_VAS_REVENUE_DETAIL' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_VAS_REVENUE_DETAIL WHERE TRANSACTION_DATE='###SLICE_VALUE###'
UNION ALL

SELECT 'FTA' table_type , 'MSC/IN' table_source, 'FT_A_GPRS_LOCATION' TABLE_NAME, COUNT(*) NB_ROWS, MAX(insert_date) TABLE_INSERT_DATE  FROM AGG.FT_A_GPRS_LOCATION WHERE session_date='###SLICE_VALUE###'
UNION ALL
SELECT 'FTA' table_type , 'MSC/IN' table_source, 'FT_A_CREDIT_TRANSFER_REVENUE' TABLE_NAME, COUNT(*) NB_ROWS, MAX(last_update) TABLE_INSERT_DATE  FROM AGG.FT_A_CREDIT_TRANSFER_REVENUE WHERE refill_date='###SLICE_VALUE###'
UNION ALL
SELECT 'FTA' table_type , 'MSC/IN' table_source, 'FT_A_REFILL_REVENUE' TABLE_NAME, COUNT(*) NB_ROWS, MAX(last_update) TABLE_INSERT_DATE  FROM AGG.FT_A_REFILL_REVENUE WHERE refill_date='###SLICE_VALUE###'
--UNION ALL
--SELECT 'FTA' table_type , 'MSC/IN' table_source, 'FT_A_REFILL_RECEIVER' TABLE_NAME, COUNT(*) NB_ROWS, MAX(last_update) TABLE_INSERT_DATE  FROM AGG.FT_A_REFILL_RECEIVER WHERE TO_DATE(last_update)>='###SLICE_VALUE###' and REFILL_MONTH = DATE_FORMAT('###SLICE_VALUE###','yyyy-MM')
UNION ALL
SELECT 'FTA' table_type , 'MSC/IN' table_source, 'FT_A_SUBSCRIBER_SUMMARY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(refresh_date) TABLE_INSERT_DATE  FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE datecode='###SLICE_VALUE###'
UNION ALL
SELECT 'FTA' table_type , 'MSC/IN' table_source, 'FT_A_VAS_REVENUE_DAILY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM AGG.FT_A_VAS_REVENUE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_IMEI_ONLINE' TABLE_NAME, COUNT(*) NB_ROWS, MAX(insert_date) TABLE_INSERT_DATE  FROM MON.FT_IMEI_ONLINE WHERE SDATE='###SLICE_VALUE###'
--UNION ALL
--SELECT 'FT' table_type , 'OM' table_source, 'FT_OM_APGL_TRANSACTION' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OM_APGL_TRANSACTION WHERE DOCUMENT_DATE='###SLICE_VALUE###'
--UNION ALL
--SELECT 'FT' table_type , 'OM' table_source, 'FT_OMNY_GLOBAL_ACTIVITY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OMNY_GLOBAL_ACTIVITY WHERE event_date='###SLICE_VALUE###'
-- UNION ALL
-- SELECT 'FT' table_type , 'OM' table_source, 'FT_OMNY_ACCOUNT_SNAPSHOT' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OMNY_ACCOUNT_SNAPSHOT WHERE event_date='###SLICE_VALUE###'
-- UNION ALL
-- SELECT 'FT' table_type , 'OM' table_source, 'FT_OMNY_BALANCE' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OMNY_BALANCE WHERE event_date='###SLICE_VALUE###'
-- UNION ALL
-- SELECT 'FT' table_type , 'OM' table_source, 'FT_OMNY_BALANCE_SNAPSHOT' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OMNY_BALANCE_SNAPSHOT WHERE event_date='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'OM' table_source, 'ACTIVATION_ALL_BY_DAY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.ACTIVATION_ALL_BY_DAY WHERE event_date='###SLICE_VALUE###'
--UNION ALL
--SELECT 'FT' table_type , 'OM' table_source, 'FT_OM_BICEC_TRANSACTION' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_OM_BICEC_TRANSACTION WHERE event_date='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'MSC/IN' table_source, 'FT_OTHER_VAS' TABLE_NAME, COUNT(*) NB_ROWS, MAX(DWH_ENTRY_DATE) TABLE_INSERT_DATE  FROM MON.FT_OTHER_VAS WHERE TRANSACTION_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'IN' table_source, 'FT_SUBSCRIPTION_MSISDN_DAY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_SUBSCRIPTION_MSISDN_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'IN' table_source, 'FT_DATA_CONSO_MSISDN_DAY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_DATA_CONSO_MSISDN_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
UNION ALL
SELECT 'FT' table_type , 'IN' table_source, 'FT_CONSO_MSISDN_DAY' TABLE_NAME, COUNT(*) NB_ROWS, MAX(INSERT_DATE) TABLE_INSERT_DATE  FROM MON.FT_CONSO_MSISDN_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
) T
