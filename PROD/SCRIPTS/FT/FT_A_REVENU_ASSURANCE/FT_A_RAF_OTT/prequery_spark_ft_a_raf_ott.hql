SELECT IF(T_1.FT_A_RAF_OTT = 0 and T_2.FT_OTARIE_DATA_TRAFFIC_DAY > 1,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) FT_A_RAF_OTT FROM AGG.SPARK_FT_A_RAF_OTT WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_OTARIE_DATA_TRAFFIC_DAY FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2