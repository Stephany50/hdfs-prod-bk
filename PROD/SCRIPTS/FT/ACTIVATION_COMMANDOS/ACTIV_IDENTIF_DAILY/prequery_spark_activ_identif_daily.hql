SELECT IF(T_1.FT_COUNT = 0 AND T_2.FT_AM > 1 AND T_3.FT_DT > 5 and FT_FAS > 5 ,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) FT_COUNT FROM MON.SPARK_ACTIV_IDENTIF_DAILY WHERE EVENT_DATE= '###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_AM FROM MON.SPARK_FT_ACTIVATION_MONTH1 WHERE EVENT_MONTH = substr('###SLICE_VALUE###',1,7) and ACTIVATION_DATE='###SLICE_VALUE###') T_2,
    (SELECT COUNT(*) FT_DT FROM DIM.SPARK_DT_BASE_IDENTIFICATION WHERE DATE_IDENTIFICATION = '###SLICE_VALUE###')T_3,
    (SELECT COUNT(*) FT_FAS FROM SPARK_FT_FAKE_ACTIVATION_STATUS WHERE EVENT_DATE = '###SLICE_VALUE###')T_4