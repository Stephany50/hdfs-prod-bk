SELECT IF(
T_1.FT_EXIST = 0 AND
T_2.POST1_CONTRACT_SNAPSHOT>0 AND
T_3.CLIENT_LAC_TRAFFIC_DAY>0 AND
T_4.POST1_CLIENT_LAC_TRAFFIC_DAY>0 AND
T_5.POST2_CLIENT_LAC_TRAFFIC_DAY>0
,"OK","NOK") ACTIV_LOC_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_ACTIVATIONS_LOCATION WHERE ACTIVATION_DATE ='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) POST1_CONTRACT_SNAPSHOT FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',2)) T_2,
(SELECT COUNT(*) CLIENT_LAC_TRAFFIC_DAY FROM MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) POST1_CLIENT_LAC_TRAFFIC_DAY FROM MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1)) T_4,
(SELECT COUNT(*) POST2_CLIENT_LAC_TRAFFIC_DAY FROM MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',2)) T_5