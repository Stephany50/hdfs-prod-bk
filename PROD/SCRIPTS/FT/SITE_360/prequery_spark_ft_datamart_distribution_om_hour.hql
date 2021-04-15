SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 1
    AND T_3.IT_EXIST > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) IT_EXIST FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME='###SLICE_VALUE###') T_3