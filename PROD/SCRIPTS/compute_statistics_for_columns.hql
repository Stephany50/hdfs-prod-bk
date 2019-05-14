ANALYZE TABLE CDR.IT_ZTE_ACCT_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_BAL_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_CUST_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_ACC_NBR_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_PRICE_PLAN_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_PROD_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_SUBS_EXTRACT PARTITION(ORIGINAL_FILE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_ZTE_VOICE_SMS PARTITION(START_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_CRA_MSC_HUAWEI PARTITION(CALLDATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.IT_SMSC_MVAS_A2P PARTITION(WRITE_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE CDR.FT_CONTRACT_SNAPSHOT PARTITION(EVENT_DATE='###SLICE_VALUE###') COMPUTE STATISTICS FOR COLUMNS;
