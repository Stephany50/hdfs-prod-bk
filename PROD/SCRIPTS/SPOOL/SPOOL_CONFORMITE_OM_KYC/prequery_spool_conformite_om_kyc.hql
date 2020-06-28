SELECT IF(T_1.SPOOL_COUNT = 0 AND T_2.FT_COUNTA > 0 AND T_3.FT_COUNTB > 0 AND T_4.FT_COUNTC > 0 ,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_CONFORMITE_OM_KYC WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_COUNTA FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
    (SELECT COUNT(*) FT_COUNTB FROM MON.SPARK_FT_OM_ACTIVE_USER WHERE EVENT_DATE='###SLICE_VALUE###') T_3,
    (SELECT COUNT(*) FT_COUNTC FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') T_4
