SELECT IF(
    T_1.FT_SITE_360_EXIST = 0
    AND T_2.FT_CELL_360_EXIST > 1
    AND T_3.FT_REFILL_EXIST > 1
    AND T_4.FT_CREDIT_TRANSFER_EXIST > 1
    AND T_5.FT_CLIENT_LAST_SITE_DAY_EXIST > 1
    AND T_6.FT_CONTRACT_SNAPSHOT_COUNT > 1
    AND T_7.FT_ACCOUNT_ACTIVITY_COUNT > 1
    --AND T_8.FT_OMNY_ACCOUNT_SNAPSHOT_COUNT > 1
    AND T_9.IT_OMNY_TRANSACTION_COUNT > 1
    AND T_10.FT_SUBSCRIPTION_COUNT > 1
    AND T_11.FT_IMEI_ONLINE_COUNT > 1
    AND T_12.FT_EMERGENCY_DATA_COUNT > 1
    AND T_13.FT_DATA_TRANSFER_COUNT > 1
    AND T_14.FT_CBM_BUNDLE_SUB_COUNT > 1
    AND T_15.FT_PDM_SITE_DAY_COUNT > 1
    AND T_16.FT_MSC_TRANSACTION > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_SITE_360_EXIST FROM MON.SPARK_FT_SITE_360 WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_CELL_360_EXIST FROM MON.SPARK_FT_CELL_360 WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_REFILL_EXIST FROM MON.SPARK_FT_REFILL WHERE REFILL_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_CREDIT_TRANSFER_EXIST FROM MON.SPARK_FT_CREDIT_TRANSFER WHERE REFILL_DATE='###SLICE_VALUE###') T_4,
(SELECT COUNT(*) FT_CLIENT_LAST_SITE_DAY_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='###SLICE_VALUE###') T_5,
(SELECT COUNT(*) FT_CONTRACT_SNAPSHOT_COUNT FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE=DATE_ADD('###SLICE_VALUE###',1)) T_6,
(SELECT COUNT(*) FT_ACCOUNT_ACTIVITY_COUNT FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE=DATE_ADD('###SLICE_VALUE###',1)) T_7,
--(SELECT COUNT(*) FT_OMNY_ACCOUNT_SNAPSHOT_COUNT FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') T_8,
(SELECT COUNT(*) IT_OMNY_TRANSACTION_COUNT FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME='###SLICE_VALUE###') T_9,
(SELECT COUNT(*) FT_SUBSCRIPTION_COUNT FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_10,
(SELECT COUNT(*) FT_IMEI_ONLINE_COUNT FROM MON.SPARK_FT_IMEI_ONLINE WHERE SDATE='###SLICE_VALUE###') T_11,
(SELECT COUNT(*) FT_EMERGENCY_DATA_COUNT FROM MON.SPARK_FT_EMERGENCY_DATA WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_12,
(SELECT COUNT(*) FT_DATA_TRANSFER_COUNT FROM MON.SPARK_FT_DATA_TRANSFER WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_13,
(SELECT COUNT(*) FT_CBM_BUNDLE_SUB_COUNT FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE PERIOD='###SLICE_VALUE###') T_14,
(SELECT COUNT(*) FT_PDM_SITE_DAY_COUNT FROM MON.SPARK_FT_PDM_SITE_DAY WHERE EVENT_DATE='###SLICE_VALUE###') T_15,
(SELECT COUNT(*) FT_MSC_TRANSACTION FROM MON.SPARK_FT_MSC_TRANSACTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_16
