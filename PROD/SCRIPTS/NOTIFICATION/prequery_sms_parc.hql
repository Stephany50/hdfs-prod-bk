SELECT  IF(T_1.SMS_EXISTS = 0 AND T_6.SMS_PREV_EXISTS>=1 AND T_2.FT_GROUP_SUBSCRIBER_SUMMARY_OK>=1 AND T_3.FT_GROUP_SUBSCRIBER_SUMMARY_PRE_OK >=1 AND T_4.FT_A_SUBSCRIBER_SUMMARY_OK >=1 AND T_5.FT_COMMERCIAL_SUBSCRIB_SUMMARY_OK >=1,"OK","NOK") REVENUE_EXISTS
FROM
(SELECT COUNT(*) SMS_EXISTS FROM MON.SMS_PARC WHERE SDATE=DATE_SUB('###SLICE_VALUE###',1)) T_1,
(SELECT COUNT(*) SMS_PREV_EXISTS FROM MON.SMS_PARC WHERE SDATE=DATE_SUB('###SLICE_VALUE###',2)) T_6,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY_OK  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY_PRE_OK  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_3,
(SELECT COUNT(*) FT_A_SUBSCRIBER_SUMMARY_OK FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE DATECODE =DATE_SUB('###SLICE_VALUE###',1))T_4 ,
(SELECT COUNT(*) FT_COMMERCIAL_SUBSCRIB_SUMMARY_OK FROM MON.SPARK_FT_COMMERCIAL_SUBSCRIB_SUMMARY WHERE DATECODE = DATE_SUB('###SLICE_VALUE###',1)) T_5