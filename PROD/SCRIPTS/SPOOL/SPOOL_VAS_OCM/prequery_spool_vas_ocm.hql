SELECT IF(T_1.SPOOL_COUNT = 0 AND T_2.FT_COUNTA > 0 ,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_VAS_OCM WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_COUNTA FROM MON.SPARK_FT_VAS_REVENUE_DETAIL WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2
