CREATE  TABLE IF NOT EXISTS AGG.REVENUE_SUMMARY_DAILY(
    EVENT_DATE DATE
    , IN_NBR_GSM_VOX BIGINT
    , IN_GSM_VOX_VOL BIGINT
    , IN_GSM_VOX_TAX_AMT DOUBLE
    , IN_NBR_GSM_SMS BIGINT
    , IN_GSM_SMS_TAX_AMT DOUBLE
    , IN_NBR_GSM_VOX_POST BIGINT
    , IN_GSM_VOX_POST_VOL BIGINT
    , IN_GSM_VOX_POST_TAX_AMT DOUBLE
    , IN_NBR_GSM_SMS_POST BIGINT
    , IN_GSM_SMS_POST_TAX_AMT DOUBLE
    , IN_NBR_SUBSC_BUN_SMS BIGINT
    , IN_SUBSC_BUN_SMS_TAX_AMT DOUBLE
    , IN_NBR_SUBSC_MOD_FNF BIGINT
    , IN_SUBSC_MOD_FNF_TAX_AMT DOUBLE
    , IN_NBR_SUBSC_CHG_BRAND BIGINT
    , IN_SUBSC_CHG_BRAND_TAX_AMT DOUBLE
    , IN_NBR_SUBSC_USS BIGINT
    , IN_SUBSC_USS_TAX_AMT DOUBLE
    , IN_NBR_SUBSC_BUN_VOX BIGINT
    , IN_SUBSC_BUN_VOX_TAX_AMT DOUBLE
    , IN_NBR_SOS_CREDIT BIGINT
    , IN_SOS_CREDIT_TAX_AMT DOUBLE
    , IN_NBR_SOS_DATA BIGINT
    , IN_SOS_DATA_TAX_AMT DOUBLE
    , IN_NBR_ADJ_RBT BIGINT
    , IN_ADJ_RBT_TAX_AMT DOUBLE
    , IN_NBR_ADJ_USS BIGINT
    , IN_ADJ_USS_TAX_AMT DOUBLE
    , IN_NBR_ADJ_VOI_SMS BIGINT
    , IN_ADJ_VOI_SMS_TAX_AMT DOUBLE
    , IN_NBR_ADJ_VEXT BIGINT
    , IN_ADJ_VEXT_TAX_AMT DOUBLE
    , IN_NBR_ADJ_PAR BIGINT
    , IN_ADJ_PAR_TAX_AMT DOUBLE
    , IN_NBR_ADJ_FBO BIGINT
    , IN_ADJ_FBO_TAX_AMT DOUBLE
    , IN_NBR_ADJ_CEL BIGINT
    , IN_ADJ_CEL_TAX_AMT DOUBLE
    , IN_NBR_ADJ_SIG BIGINT
    , IN_ADJ_SIG_TAX_AMT DOUBLE
    , IN_NBR_DEAC_ACCT_BAL BIGINT
    , IN_DEAC_ACCT_BAL_TAX_AMT DOUBLE
    , IN_NBR_GPRS_SVA BIGINT
    , IN_GPRS_SVA_VOL BIGINT
    , IN_GPRS_SVA_TAX_AMT DOUBLE
    , IN_NBR_GPRS_PAYGO BIGINT
    , IN_GPRS_PAYGO_VOL BIGINT
    , IN_GPRS_PAYGO_TAX_AMT DOUBLE
    , IN_NBR_GPRS_SVA_POST BIGINT
    , IN_GPRS_SVA_POST_VOL BIGINT
    , IN_GPRS_SVA_POST_TAX_AMT DOUBLE
    , IN_NBR_GPRS_PAYGO_POST BIGINT
    , IN_GPRS_PAYGO_POST_VOL BIGINT
    , IN_GPRS_PAYGO_POST_TAX_AMT DOUBLE
    , IN_NBR_REFILL_TOPUP BIGINT
    , IN_REFILL_TOPUP_TAX_AMT DOUBLE
    , IN_NBR_DATA_TRANS BIGINT
    , IN_DATA_TRANS_TAX_AMT DOUBLE
    , IN_NBR_VAS_DATA BIGINT
    , IN_VAS_DATA_TAX_AMT DOUBLE
    , ZEBRA_NBR_C2S BIGINT
    , ZEBRA_C2S_TAX_AMT DOUBLE
    , TANGO_NBR_OM_DATA BIGINT
    , TANGO_OM_DATA_TAX_AMT DOUBLE
    , P2P_NBR_TRANS_FEES BIGINT
    , P2P_TRANS_FEES_TAX_AMT DOUBLE
    , P2P_NBR_CREDIT_TRANS BIGINT
    , P2P_CREDIT_TRANS_TAX_AMT DOUBLE
) COMMENT 'Revenue Summary Daily external table for hbase'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key
                        ,IN:NBR_GSM_VOX
                        ,IN:GSM_VOX_VOL 
                        ,IN:GSM_VOX_TAX_AMT 
                        ,IN:NBR_GSM_SMS 
                        ,IN:GSM_SMS_TAX_AMT 
                        ,IN:NBR_GSM_VOX_POST 
                        ,IN:GSM_VOX_POST_VOL 
                        ,IN:GSM_VOX_POST_TAX_AMT 
                        ,IN:NBR_GSM_SMS_POST 
                        ,IN:GSM_SMS_POST_TAX_AMT 
                        ,IN:NBR_SUBSC_BUN_SMS 
                        ,IN:SUBSC_BUN_SMS_TAX_AMT 
                        ,IN:NBR_SUBSC_MOD_FNF 
                        ,IN:SUBSC_FNF_TAX_AMT 
                        ,IN:NBR_SUBSC_CHG_BRAND 
                        ,IN:SUBSC_CHG_BRAND_TAX_AMT 
                        ,IN:NBR_SUBSC_USS 
                        ,IN:SUBSC_USS_TAX_AMT 
                        ,IN:NBR_SUBSC_BUN_VOX 
                        ,IN:SUBSC_BUN_VOX_TAX_AMT 
                        ,IN:NBR_SOS_CREDIT 
                        ,IN:SOS_CREDIT_TAX_AMT 
                        ,IN:NBR_SOS_DATA 
                        ,IN:SOS_DATA_TAX_AMT 
                        ,IN:NBR_ADJ_RBT 
                        ,IN:ADJ_RBT_TAX_AMT 
                        ,IN:NBR_ADJ_USS 
                        ,IN:ADJ_USS_TAX_AMT 
                        ,IN:NBR_ADJ_VOI_SMS 
                        ,IN:ADJ_VOI_SMS_TAX_AMT 
                        ,IN:NBR_ADJ_VEXT 
                        ,IN:ADJ_VEXT_TAX_AMT 
                        ,IN:NBR_ADJ_PAR 
                        ,IN:ADJ_PAR_TAX_AMT 
                        ,IN:NBR_ADJ_FBO 
                        ,IN:ADJ_FBO_TAX_AMT 
                        ,IN:NBR_ADJ_CEL 
                        ,IN:ADJ_CEL_TAX_AMT 
                        ,IN:NBR_ADJ_SIG 
                        ,IN:ADJ_SIG_TAX_AMT 
                        ,IN:NBR_DEAC_ACCT_BAL 
                        ,IN:DEAC_ACCT_BAL_TAX_AMT 
                        ,IN:NBR_GPRS_SVA 
                        ,IN:GPRS_SVA_VOL 
                        ,IN:GPRS_SVA_TAX_AMT 
                        ,IN:NBR_GPRS_PAYGO 
                        ,IN:GPRS_PAYGO_VOL 
                        ,IN:GPRS_PAYGO_TAX_AMT 
                        ,IN:NBR_GPRS_SVA_POST 
                        ,IN:GPRS_SVA_POST_VOL 
                        ,IN:GPRS_SVA_POST_TAX_AMT 
                        ,IN:NBR_GPRS_PAYGO_POST 
                        ,IN:GPRS_PAYGO_POST_VOL 
                        ,IN:GPRS_PAYGO_POST_TAX_AMT 
                        ,IN:NBR_REFILL_TOPUP 
                        ,IN:REFILL_TOPUP_TAX_AMT 
                        ,IN:NBR_DATA_TRANS 
                        ,IN:DATA_TRANS_TAX_AMT 
                        ,IN:NBR_VAS_DATA 
                        ,IN:VAS_DATA_TAX_AMT 
                        ,ZEBRA:NBR_C2S 
                        ,ZEBRA:C2S_TAX_AMT 
                        ,TANGO:NBR_OM_DATA 
                        ,TANGO:OM_DATA_TAX_AMT 
                        ,OTHER:NBR_TRANS_FEES_P2P 
                        ,OTHER:TRANS_FEES_P2P_TAX_AMT 
                        ,OTHER:NBR_CREDIT_TRANS_P2P 
                        ,OTHER:CREDIT_TRANS_P2P_TAX_AMT ")
TBLPROPERTIES ("hbase.table.name" = "report:revenue_summary_daily", "hbase.mapred.output.outputtable" = "report:revenue_summary_daily");

