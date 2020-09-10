SELECT IF(
    T_1.FT_A_EXIST = 0
    AND T_2.FT_EXIST > 1
    AND T_3.NB_INSERT_DATE= 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) FT_A_EXIST FROM AGG.SPARK_FT_A_EMERGENCY_CREDIT_ACTIVITY WHERE DATECODE= '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_EMERGENCY_CREDIT_ACTIVITY WHERE EVENT_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(distinct insert_date) NB_INSERT_DATE FROM MON.SPARK_FT_EMERGENCY_CREDIT_ACTIVITY WHERE EVENT_DATE = '###SLICE_VALUE###') T_3
