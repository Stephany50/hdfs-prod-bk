SELECT
    *
FROM MON.SPARK_FT_CBM_ARPU_MOU
WHERE EVENT_DATE='###SLICE_VALUE###' AND (NVL(MOU, 0) + NVL(ARPU, 0) + NVL(ARPU2, 0) + nvl(Parrain, 0) + NVL(ARPU_ROAMING, 0) + NVL(REF_AMT, 0) + NVL(PAYGO_WEBI, 0) + NVL(SMS_WEBI, 0) + nvl(INC_NB_CALLS, 0) + nvl(volume_voip, 0) + nvl(volume_data, 0) + nvl(volume_chat, 0) + NVL(volume_ott, 0) + nvl(vas_amt, 0) + NVL(VAS_AMT, 0) + NVL(VAS_NB, 0)) > 0