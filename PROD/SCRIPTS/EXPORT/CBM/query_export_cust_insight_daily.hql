SELECT 
    REPLACE(PERIOD, ';', ' ') period,
    REPLACE(MSISDN, ';', ' ') msisdn,
    REPLACE(NB_CALLS, ';', ' ') nb_calls,
    REPLACE(ACTUAL_DURATION, ';', ' ') actual_duration,
    REPLACE(MOU_ONNET, ';', ' ') mou_onnet,
    REPLACE(MOU_OFNET, ';', ' ') mou_ofnet,
    REPLACE(MOU_INTER, ';', ' ') mou_inter,
    REPLACE(MOU_ON_PEAK, ';', ' ') mou_on_peak,
    REPLACE(MOU_OF_PEAK, ';', ' ') mou_of_peak,
    REPLACE(MOU_INT_PEAK, ';', ' ') mou_int_peak,
    REPLACE(MOU_ON_OFFPEAK, ';', ' ') mou_on_offpeak,
    REPLACE(MOU_OF_OFFPEAK, ';', ' ') mou_of_offpeak,
    REPLACE(MOU_INT_OFFPEAK, ';', ' ') mou_int_offpeak,
    REPLACE(FOU_SMS, ';', ' ') fou_sms,
    REPLACE(FOU_SMS_ONNET, ';', ' ') fou_sms_onnet,
    REPLACE(FOU_SMS_OFNET, ';', ' ') fou_sms_ofnet,
    REPLACE(FOU_SMS_INTERNAT, ';', ' ') fou_sms_internat,
    REPLACE(SOI_ONNET, ';', ' ') soi_onnet,
    REPLACE(SOI_OFNET, ';', ' ') soi_ofnet,
    REPLACE(SOI_INTER, ';', ' ') soi_inter,
    REPLACE(SOR_ONNET, ';', ' ') sor_onnet,
    REPLACE(SOR_OFNET, ';', ' ') sor_ofnet,
    REPLACE(SOR_INTER, ';', ' ') sor_inter,
    REPLACE(MA_VOICE_ONNET, ';', ' ') ma_voice_onnet,
    REPLACE(MA_VOICE_OFNET, ';', ' ') ma_voice_ofnet,
    REPLACE(MA_VOICE_INTER, ';', ' ') ma_voice_inter,
    REPLACE(MA_SMS_ONNET, ';', ' ') ma_sms_onnet,
    REPLACE(MA_SMS_OFNET, ';', ' ') ma_sms_ofnet,
    REPLACE(MA_SMS_INTER, ';', ' ') ma_sms_inter,
    REPLACE(MA_DATA, ';', ' ') ma_data,
    REPLACE(MA_VAS, ';', ' ') ma_vas,
    REPLACE(MA_GOS_SVA, ';', ' ') ma_gos_sva,
    REPLACE(MA_VOICE_ROAMING, ';', ' ') ma_voice_roaming,
    REPLACE(MA_SMS_ROAMING, ';', ' ') ma_sms_roaming,
    REPLACE(INC_NB_CALLS, ';', ' ') inc_nb_calls,
    REPLACE(INC_CALL_DURATION, ';', ' ') inc_call_duration,
    REPLACE(MA_VOICE_SVA, ';', ' ') ma_voice_sva,
    REPLACE(MA_SMS_SVA, ';', ' ') ma_sms_sva
FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY
WHERE PERIOD = "###SLICE_VALUE###"