SELECT IF(
    T_1.FT_EXIST = 0
    AND T_4.FT_PREV_EXIST > 1
    AND T_2.FT_CRA_GPRS_EXIST > 1
    AND T_3.FT_msc_transaction_EXIST > 1
    AND T_5.FT_contract_snapshot_EXIST > 1
    AND T_6.FT_account_activity_EXIST > 1
    AND T_7.FT_og_ic_call_EXIST > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.spark_ft_client_cell_traffic_hour WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_PREV_EXIST FROM MON.spark_ft_client_cell_traffic_hour WHERE EVENT_DATE=date_sub('###SLICE_VALUE###', 1)) T_4,
(SELECT COUNT(*) FT_CRA_GPRS_EXIST FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_msc_transaction_EXIST FROM MON.SPARK_FT_msc_transaction WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_contract_snapshot_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE event_date=DATE_ADD('###SLICE_VALUE###',1)) T_5,
(SELECT COUNT(*) FT_account_activity_EXIST FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE event_date=DATE_ADD('###SLICE_VALUE###',1)) T_6,
(SELECT COUNT(*) FT_og_ic_call_EXIST FROM MON.SPARK_FT_OG_IC_CALL_SNAPSHOT WHERE event_date=DATE_ADD('###SLICE_VALUE###',1)) T_7
