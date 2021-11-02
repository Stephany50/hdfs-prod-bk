SELECT
    period,
    msisdn,
    nb_calls,
    actual_duration,
    mou_onnet,
    mou_ofnet,
    mou_inter,
    mou_on_peak,
    mou_of_peak,
    mou_int_peak,
    mou_on_offpeak,
    mou_of_offpeak,
    mou_int_offpeak,
    inc_nb_calls,
    inc_onnet_nb_calls,
    inc_ofnet_nb_calls,
    inc_inter_nb_calls,
    fou_sms,
    fou_sms_onnet,
    fou_sms_ofnet,
    fou_sms_internat,
    inc_nb_sms,
    nb_day_calls,
    nb_day_onnet_calls,
    nb_day_ofnet_calls,
    nb_day_int_calls,
    nb_day_sms,
    nb_day_data,
    soi_onnet,
    soi_ofnet,
    soi_inter,
    sor_onnet,
    sor_ofnet,
    sor_inter,
    ma_voice_onnet,
    ma_voice_ofnet,
    ma_voice_inter,
    ma_sms_onnet,
    ma_sms_ofnet,
    ma_sms_inter,
    ma_data,
    ma_vas,
    ma_gos_sva,
    ma_voice_roaming,
    ma_sms_roaming,
    ma_voice_sva,
    ma_sms_sva,
    
    bytes_data
FROM MON.SPARK_FT_CBM_CUST_INSIGTH_MONTHLY
WHERE PERIOD = '###SLICE_VALUE###'