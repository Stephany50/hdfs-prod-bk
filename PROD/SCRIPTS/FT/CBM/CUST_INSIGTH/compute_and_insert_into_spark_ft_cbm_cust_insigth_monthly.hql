INSERT INTO MON.SPARK_FT_CBM_CUST_INSIGTH_MONTHLY
SELECT 
    MSISDN,
    COALESCE(SUM(NB_CALLS), 0) AS NB_CALLS,
    COALESCE(SUM(ACTUAL_DURATION), 0) AS ACTUAL_DURATION,
    COALESCE(SUM(MOU_ONNET), 0) AS MOU_ONNET,
    COALESCE(SUM(MOU_OFNET), 0) AS MOU_OFNET,
    COALESCE(SUM(MOU_INTER), 0) AS MOU_INTER,
    COALESCE(SUM(MOU_ON_PEAK), 0) AS MOU_ON_PEAK,
    COALESCE(SUM(MOU_OF_PEAK), 0) AS MOU_OF_PEAK,
    COALESCE(SUM(MOU_INT_PEAK), 0) AS MOU_INT_PEAK,
    COALESCE(SUM(MOU_ON_OFFPEAK), 0) AS MOU_ON_OFFPEAK,
    COALESCE(SUM(MOU_OF_OFFPEAK), 0) AS MOU_OF_OFFPEAK,
    COALESCE(SUM(MOU_INT_OFFPEAK), 0) AS MOU_INT_OFFPEAK,
    COALESCE(SUM(INC_NB_CALLS), 0) AS INC_NB_CALLS,
    COALESCE(SUM(INC_ONNET_NB_CALLS), 0) AS INC_ONNET_NB_CALLS,
    COALESCE(SUM(INC_OFNET_NB_CALLS), 0) AS INC_OFNET_NB_CALLS,
    COALESCE(SUM(INC_INTER_NB_CALLS), 0) AS INC_INTER_NB_CALLS,
    COALESCE(SUM(FOU_SMS), 0) AS FOU_SMS,
    COALESCE(SUM(FOU_SMS_ONNET), 0) AS FOU_SMS_ONNET,
    COALESCE(SUM(FOU_SMS_OFNET), 0) AS FOU_SMS_OFNET,
    COALESCE(SUM(FOU_SMS_INTERNAT), 0) AS FOU_SMS_INTERNAT,
    COALESCE(SUM(INC_NB_SMS), 0) AS INC_NB_SMS,
    SUM(CASE WHEN NB_CALLS > 0 THEN 1 ELSE 0 END) AS NB_DAY_CALLS,
    SUM(CASE WHEN MOU_ONNET > 0 THEN 1 ELSE 0 END) AS NB_DAY_ONNET_CALLS,
    SUM(CASE WHEN MOU_OFNET > 0 THEN 1 ELSE 0 END) AS NB_DAY_OFNET_CALLS,
    SUM(CASE WHEN MOU_INTER > 0 THEN 1 ELSE 0 END) AS NB_DAY_INT_CALLS,
    SUM(CASE WHEN FOU_SMS > 0 THEN 1 ELSE 0 END) AS NB_DAY_SMS,
    SUM(CASE WHEN BYTES_DATA > 0 THEN 1 ELSE 0 END) AS NB_DAY_DATA,
    NULL AS SOI_ONNET,
    NULL AS SOI_OFNET,
    NULL AS SOI_INTER,
    NULL AS SOR_ONNET,
    NULL AS SOR_OFNET,
    NULL AS SOR_INTER,
    COALESCE(SUM(MA_VOICE_ONNET), 0) AS MA_VOICE_ONNET,
    COALESCE(SUM(MA_VOICE_OFNET), 0) AS MA_VOICE_OFNET,
    COALESCE(SUM(MA_VOICE_INTER), 0) AS MA_VOICE_INTER,
    COALESCE(SUM(MA_SMS_ONNET), 0) AS MA_SMS_ONNET,
    COALESCE(SUM(MA_SMS_OFNET), 0) AS MA_SMS_OFNET,
    COALESCE(SUM(MA_SMS_INTER), 0) AS MA_SMS_INTER,
    COALESCE(SUM(MA_DATA), 0) AS MA_DATA,
    COALESCE(SUM(MA_VAS), 0) AS MA_VAS,
    CURRENT_TIMESTAMP() AS INSERT_DATE,
    COALESCE(SUM(MA_GOS_SVA), 0) AS MA_GOS_SVA,
    COALESCE(SUM(MA_VOICE_ROAMING), 0) AS MA_VOICE_ROAMING,
    COALESCE(SUM(MA_VOICE_SVA), 0) AS MA_VOICE_SVA,
    COALESCE(SUM(MA_SMS_ROAMING), 0) AS MA_SMS_ROAMING,
    COALESCE(SUM(MA_SMS_SVA), 0) AS MA_SMS_SVA,
    SUM(BYTES_DATA) BYTES_DATA,
    '###SLICE_VALUE###' AS PERIOD
FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY
WHERE PERIOD BETWEEN '###SLICE_VALUE###'||'-01' AND DATE_SUB(ADD_MONTHS('###SLICE_VALUE###'||'-01', 1), 1)
GROUP BY MSISDN
