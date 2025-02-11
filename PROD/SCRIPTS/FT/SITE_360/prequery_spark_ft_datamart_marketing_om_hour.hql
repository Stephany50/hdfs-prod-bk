SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 1
    AND T_3.IT_EXIST > 1
    AND T_4.IT_EXIST > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) IT_EXIST FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) IT_EXIST FROM cdr.spark_it_om_all_users WHERE original_file_date='###SLICE_VALUE###') T_4