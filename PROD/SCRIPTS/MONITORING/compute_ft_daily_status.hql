
INSERT INTO FT_DAILY_STATUS
select '###SLICE_VALUE###' TABLE_DATE, TABLE_TYPE, TABLE_SOURCE, TABLE_NAME, '' DEPENDANCES, IF(NB_ROWS < 1, 'NOK', 'OK') STATUT, NB_ROWS, null LAST_INCIDENT_DAY, TABLE_INSERT_DATE, current_timestamp INSERT_DATE
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
SELECT 'FT' table_type, 'IN' table_source, 'FT_SUBSCRIPTION' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_SUBSCRIPTION where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_DATA_TRANSFER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_DATA_TRANSFER where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_EMERGENCY_DATA' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_EMERGENCY_DATA where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_OVERDRAFT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_OVERDRAFT where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_CONTRACT_SNAPSHOT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_CONTRACT_SNAPSHOT where event_date = '###SLICE_VALUE###'
union
SELECT 'FT' table_type, 'IN' table_source, 'FT_CREDIT_TRANSFER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_CREDIT_TRANSFER where refill_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_DATA_TRANSFER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_A_DATA_TRANSFER where event_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_GPRS_ACTIVITY' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_A_GPRS_ACTIVITY where datecode = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_GPRS_ACTIVITY_POST' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_A_GPRS_ACTIVITY_POST where datecode = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_A_SUBSCRIPTION' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_A_SUBSCRIPTION where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_GSM_TRAFFIC_REVENUE_DAILY' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_GSM_TRAFFIC_REVENUE_DAILY where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'IN' table_source, 'FT_GSM_TRAFFIC_REVENUE_POST' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_GSM_TRAFFIC_REVENUE_POST where transaction_date = '###SLICE_VALUE###'

union
SELECT 'FT' table_type, 'MSC' table_source, 'FT_MSC_TRANSACTION' table_name, count(*) nb_rows, max(ft_insert_date) table_insert_date  from MON.FT_MSC_TRANSACTION where transaction_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'MSC' table_source, 'FT_AG_INTERCO' table_name, count(*) nb_rows, max(INSERTED_DATE) table_insert_date  from MON.FT_AG_INTERCO where sdate = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'MSC' table_source, 'FT_X_INTERCO_FINAL' table_name, count(*) nb_rows, max(INSERTED_DATE) table_insert_date  from MON.FT_X_INTERCO_FINAL where sdate = '###SLICE_VALUE###'

union
SELECT 'FT' table_type, 'MVAS' table_source, 'FT_SMSC_TRANSACTION_A2P' table_name, count(*) nb_rows, max(ft_insert_date) table_insert_date  from MON.FT_SMSC_TRANSACTION_A2P where transaction_billing_date = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'MVAS' table_source, 'FT_QOS_SMSC_SPECIAL_NUMBER' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_QOS_SMSC_SPECIAL_NUMBER where state_date = '###SLICE_VALUE###'

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
SELECT 'FTA' table_type, 'EQUATION_PREPAYEE' table_source, 'FT_A_EDR_PRPD_EQT' table_name, count(*) nb_rows, max(insert_date) table_insert_date  from MON.FT_A_EDR_PRPD_EQT where event_day = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'REPORT' table_source, 'GLOBAL_ACTIVITY' table_name, if(count(distinct source_data) <14,0,count(*))  nb_rows, max(insert_date) table_insert_date  from REPORT.FT_GLOBAL_ACTIVITY_DAILY where TRANSACTION_DATE = '###SLICE_VALUE###'
) T
