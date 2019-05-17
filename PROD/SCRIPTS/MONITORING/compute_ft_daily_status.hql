
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
SELECT 'FTA' table_type, 'REPORT' table_source, 'GLOBAL_ACTIVITY' table_name, if(count(distinct source_data) <15,0,count(*))  nb_rows, max(insert_date) table_insert_date  from REPORT.FT_GLOBAL_ACTIVITY_DAILY where TRANSACTION_DATE = '###SLICE_VALUE###'
union
SELECT 'FTA' table_type, 'REPORT' table_source, 'REVENUE_SUMMARY' table_name, if(IN_NBR_GSM_VOX IS NOT NULL AND IN_GSM_VOX_VOL IS NOT NULL AND IN_GSM_VOX_TAX_AMT IS NOT NULL AND IN_NBR_GSM_SMS IS NOT NULL AND IN_GSM_SMS_TAX_AMT IS NOT NULL AND IN_NBR_GSM_VOX_POST IS NOT NULL AND IN_GSM_VOX_POST_VOL IS NOT NULL AND IN_GSM_VOX_POST_TAX_AMT IS NOT NULL AND IN_NBR_GSM_SMS_POST IS NOT NULL AND IN_GSM_SMS_POST_TAX_AMT IS NOT NULL AND IN_NBR_SUBSC_BUN_SMS IS NOT NULL AND IN_SUBSC_BUN_SMS_TAX_AMT IS NOT NULL AND IN_NBR_SUBSC_MOD_FNF IS NOT NULL AND IN_SUBSC_MOD_FNF_TAX_AMT IS NOT NULL AND IN_NBR_SUBSC_CHG_BRAND IS NOT NULL AND IN_SUBSC_CHG_BRAND_TAX_AMT IS NOT NULL AND IN_NBR_SUBSC_USS IS NOT NULL AND IN_SUBSC_USS_TAX_AMT IS NOT NULL AND IN_NBR_SUBSC_BUN_VOX IS NOT NULL AND IN_SUBSC_BUN_VOX_TAX_AMT IS NOT NULL AND IN_NBR_SOS_CREDIT IS NOT NULL AND IN_SOS_CREDIT_TAX_AMT IS NOT NULL AND IN_NBR_SOS_DATA IS NOT NULL AND IN_SOS_DATA_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_RBT IS NOT NULL AND IN_ADJ_RBT_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_USS IS NOT NULL AND IN_ADJ_USS_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_VOI_SMS IS NOT NULL AND IN_ADJ_VOI_SMS_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_VEXT IS NOT NULL AND IN_ADJ_VEXT_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_PAR IS NOT NULL AND IN_ADJ_PAR_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_FBO IS NOT NULL AND IN_ADJ_FBO_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_CEL IS NOT NULL AND IN_ADJ_CEL_TAX_AMT IS NOT NULL AND IN_NBR_ADJ_SIG IS NOT NULL AND IN_ADJ_SIG_TAX_AMT IS NOT NULL AND IN_NBR_DEAC_ACCT_BAL IS NOT NULL AND IN_DEAC_ACCT_BAL_TAX_AMT IS NOT NULL AND IN_NBR_GPRS_SVA IS NOT NULL AND IN_GPRS_SVA_VOL IS NOT NULL AND IN_GPRS_SVA_TAX_AMT IS NOT NULL AND IN_NBR_GPRS_PAYGO IS NOT NULL AND IN_GPRS_PAYGO_VOL IS NOT NULL AND IN_GPRS_PAYGO_TAX_AMT IS NOT NULL AND IN_NBR_GPRS_SVA_POST IS NOT NULL AND IN_GPRS_SVA_POST_VOL IS NOT NULL AND IN_GPRS_SVA_POST_TAX_AMT IS NOT NULL AND IN_NBR_GPRS_PAYGO_POST IS NOT NULL AND IN_GPRS_PAYGO_POST_VOL IS NOT NULL AND IN_GPRS_PAYGO_POST_TAX_AMT IS NOT NULL AND IN_NBR_REFILL_TOPUP IS NOT NULL AND IN_REFILL_TOPUP_TAX_AMT IS NOT NULL AND IN_NBR_DATA_TRANS IS NOT NULL AND IN_DATA_TRANS_TAX_AMT IS NOT NULL AND IN_NBR_VAS_DATA IS NOT NULL AND IN_VAS_DATA_TAX_AMT IS NOT NULL AND ZEBRA_NBR_C2S IS NOT NULL AND ZEBRA_C2S_TAX_AMT IS NOT NULL AND TANGO_NBR_OM_DATA IS NOT NULL AND TANGO_OM_DATA_TAX_AMT IS NOT NULL AND P2P_NBR_TRANS_FEES IS NOT NULL AND P2P_TRANS_FEES_TAX_AMT IS NOT NULL AND P2P_NBR_CREDIT_TRANS IS NOT NULL AND P2P_CREDIT_TRANS_TAX_AMT IS NOT NULL,1,0) nb_rows, EVENT_DATE table_insert_date  from REPORT.REVENUE_SUMMARY_DAILY where EVENT_DATE = '###SLICE_VALUE###'
) T
