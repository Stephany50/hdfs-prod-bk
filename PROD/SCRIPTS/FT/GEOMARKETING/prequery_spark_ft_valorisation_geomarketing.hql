SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_ZTE_DATA_EXISTS > 1
    AND T_3.FT_ZTE_VOICE_SMS_EXISTS > 1
    AND T_4.FT_CELL_TRAFFIC_HOUR_EXISTS > 1
    AND T_5.FT_ZTE_SUBSCRIPTION_EXISTS > 1
    AND T_6.FT_ZTE_BAL_EXTRACT_EXISTS > 1
    AND T_7.FT_PREV_VALORISATION_EXISTS > 1
    AND T_8.FT_PRICE_PLAN_EXISTS > 1
    AND T_9.FT_PROD_SPEC_EXISTS > 1
    AND T_10.FT_PREV_PHOTO_EXISTS > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_FINAL_VALORISATION_HORAIRE WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_ZTE_DATA_EXISTS FROM CDR.SPARK_IT_ZTE_DATA WHERE START_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_ZTE_VOICE_SMS_EXISTS FROM CDR.SPARK_IT_ZTE_VOICE_SMS WHERE START_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_CELL_TRAFFIC_HOUR_EXISTS FROM MON.SPARK_FT_CLIENT_CELL_TRAFFIC_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_4,
(SELECT COUNT(*) FT_ZTE_SUBSCRIPTION_EXISTS FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE='###SLICE_VALUE###') T_5,
(SELECT COUNT(*) FT_ZTE_BAL_EXTRACT_EXISTS FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT WHERE ORIGINAL_FILE_DATE=DATE_ADD('###SLICE_VALUE###', 1)) T_6,
(SELECT COUNT(*) FT_PREV_VALORISATION_EXISTS FROM MON.SPARK_FT_VALORISATION_HORAIRE_DANS_SOUS_COMPTE WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###', 1)) T_7,
(SELECT COUNT(*) FT_PRICE_PLAN_EXISTS FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_8,
(SELECT COUNT(*) FT_PROD_SPEC_EXISTS FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_9,
(SELECT COUNT(*) FT_PREV_PHOTO_EXISTS FROM MON.SPARK_FT_PHOTO_JOURNALIERE_DES_SOUS_COMPTES WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###', 1)) T_10