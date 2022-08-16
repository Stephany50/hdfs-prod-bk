SELECT
    MSISDN,
    EVENT_DATE,
    ARPU
    Arpu_voix,
    arpu_onet,
    arpu_ofnet,
    arpu_inter,
    arpu_data,
    arpu_VAS,
    arpu_roaming_voix,
    arpu_roaming,
    VAS_AMT,
    VAS_NB,
    PAYG_VOIX,
    PAYG_VOIX_onnet,
    PAYG_VOIX_offnet,
    PAYG_VOIX_inter,
    PAYG_VOIX_roaming,
    MOU_ONNET,
    MOU_OFNET,
    MOU_INTER,
    mou,
    bdles_voix,
    bdles_onet,
    bdles_ofnet,
    bdles_inter,
    bdles_data,
    bdles_roaming_voix,
    bdles_roaming_data,
    PAYG_DATA,
    NB_CALLS,
    REF_AMT,
    REF_NB,
    INC_NB_CALLS,
    volume_data,
    fou_sms,
    NULL paygo_sms,
    NULL bdles_sms,
    NULL arpu_sms,
    nvl(bytes_data, 0) / (1024 * 1024) volume_data_in,
    Volume_4G,
    Volume_3G,
    Volume_2G,
    SOURCE
FROM MON.SPARK_FT_CBM_ARPU_MOU
WHERE EVENT_DATE='###SLICE_VALUE###' AND (NVL(MOU, 0) + NVL(ARPU, 0) + NVL(ARPU2, 0) + nvl(Parrain, 0) + NVL(ARPU_ROAMING, 0) + NVL(REF_AMT, 0) + NVL(PAYGO_WEBI, 0) + NVL(SMS_WEBI, 0) + nvl(INC_NB_CALLS, 0) + nvl(volume_voip, 0) + nvl(volume_data, 0) + nvl(volume_chat, 0) + NVL(volume_ott, 0) + nvl(vas_amt, 0) + NVL(VAS_AMT, 0) + NVL(VAS_NB, 0)) > 0