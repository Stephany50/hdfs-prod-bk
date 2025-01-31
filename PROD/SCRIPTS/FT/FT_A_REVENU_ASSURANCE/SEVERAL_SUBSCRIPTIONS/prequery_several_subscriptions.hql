SELECT IF(T_1.SEVERAL_SUBSCRIPTIONS = 0 and T_2.FT_SUBSCRIPTION > 1,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SEVERAL_SUBSCRIPTIONS FROM MON.SEVERAL_SUBSCRIPTIONS WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_SUBSCRIPTION FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',30) AND DATE_SUB('###SLICE_VALUE###',1)) T_2