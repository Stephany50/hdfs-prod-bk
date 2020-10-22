SELECT
    PERIOD,
    MSISDN,
    NB_CALLS,
    ACTUAL_DURATION,
    MOU_ONNET,
    MOU_OFNET,
    MOU_INTER,
    MOU_ON_PEAK,
    MOU_OF_PEAK,
    MOU_INT_PEAK,
    MOU_ON_OFFPEAK,
    MOU_OF_OFFPEAK,
    MOU_INT_OFFPEAK,
    INC_NB_CALLS,
    INC_ONNET_NB_CALLS,
    INC_OFNET_NB_CALLS,
    INC_INTER_NB_CALLS,
    FOU_SMS,
    FOU_SMS_ONNET,
    FOU_SMS_OFNET,
    FOU_SMS_INTERNAT,
    INC_NB_SMS,
    NB_DAY_CALLS,
    NB_DAY_ONNET_CALLS,
    NB_DAY_OFNET_CALLS,
    NB_DAY_INT_CALLS,
    NB_DAY_SMS,
    NB_DAY_DATA,
    SOI_ONNET,
    SOI_OFNET,
    SOI_INTER,
    SOR_ONNET,
    SOR_OFNET,
    SOR_INTER,
    MA_VOICE_ONNET,
    MA_VOICE_OFNET,
    MA_VOICE_INTER,
    MA_SMS_ONNET,
    MA_SMS_OFNET,
    MA_SMS_INTER,
    MA_DATA,
    MA_VAS,
    MA_GOS_SVA,
    MA_VOICE_ROAMING,
    MA_SMS_ROAMING,
    MA_VOICE_SVA,
    MA_SMS_SVA
FROM MON.SPARK_FT_CBM_CUST_INSIGTH_MONTHLY
WHERE PERIOD = '###SLICE_VALUE###'