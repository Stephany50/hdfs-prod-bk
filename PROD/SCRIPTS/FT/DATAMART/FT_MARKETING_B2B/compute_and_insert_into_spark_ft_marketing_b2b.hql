INSERT INTO MON.SPARK_FT_MARKETING_B2B
SELECT 
    NULL EVENT_MONTH,
    EVENT_DATE,
    a.MSISDN,
    CONTRACT_TYPE,
    a.COMPTE COMPTE_CLIENT,
    COMMERCIAL_OFFER,
    ACTIVATION_DATE,
    DEACTIVATION_DATE,
    LANG,
    OSP_STATUS,
    IMSI,
    GRP_LAST_OG_CALL,
    GRP_LAST_IC_CALL,
    GRP_REMAIN_CREDIT_MAIN,
    GRP_REMAIN_CREDIT_PROMO,
    GRP_GP_STATUS,
    LOC_SITE_NAME,
    LOC_TOWN_NAME,
    LOC_ADMINTRATIVE_REGION,
    LOC_COMMERCIAL_REGION,
    TER_TAC_CODE,
    TER_CONSTRUCTOR,
    TER_MODEL,
    TER_2G_3G_4G_COMPATIBILITY,
    TER_2G_COMPATIBILITY,
    TER_3G_COMPATIBILITY,
    TER_4G_COMPATIBILITY,
    DIR_FIRST_NAME,
    DIR_LAST_NAME,
    DIR_BIRTH_DATE,
    DIR_IDENTIFICATION_TOWN,
    DIR_IDENTIFICATION_DATE,
    TOTAL_REVENUE,
    TOTAL_VOICE_REVENUE,
    TOTAL_SMS_REVENUE,
    TOTAL_SUBS_REVENUE,
    TOTAL_DATA_REVENUE,
    ROAM_IN_VOICE_REVENUE,
    ROAM_OUT_VOICE_REVENUE,
    NULL AIRTIME_DEPOT,
    ROAM_IN_SMS_REVENUE,
    ROAM_OUT_SMS_REVENUE,
    ROAM_DATA_REVENUE,
    C2S_REFILL_COUNT,
    C2S_MAIN_REFILL_AMOUNT,
    C2S_PROMO_REFILL_AMOUNT,
    P2P_REFILL_COUNT,
    P2P_REFILL_AMOUNT,
    SCRATCH_REFILL_COUNT,
    SCRATCH_MAIN_REFILL_AMOUNT,
    SCRATCH_PROMO_REFILL_AMOUNT,
    P2P_REFILL_FEES,
    OG_CALL_TOTAL_COUNT,
    OG_CALL_OCM_COUNT,
    OG_CALL_MTN_COUNT,
    OG_CALL_NEXTTEL_COUNT,
    OG_CALL_CAMTEL_COUNT,
    OG_CALL_SET_COUNT,
    OG_CALL_ROAM_IN_COUNT,
    OG_CALL_ROAM_OUT_COUNT,
    OG_CALL_SVA_COUNT,
    OG_CALL_INTERNATIONAL_COUNT,
    OG_RATED_CALL_DURATION,
    OG_TOTAL_CALL_DURATION,
    RATED_TEL_OCM_DURATION,
    RATED_TEL_MTN_DURATION,
    RATED_TEL_NEXTTEL_DURATION,
    RATED_TEL_CAMTEL_DURATION,
    RATED_TEL_SET_DURATION,
    RATED_TEL_ROAM_IN_DURATION,
    RATED_TEL_ROAM_OUT_DURATION,
    RATED_TEL_SVA_DURATION,
    RATED_TEL_INT_DURATION,
    MAIN_RATED_TEL_AMOUNT,
    PROMO_RATED_TEL_AMOUNT,
    OG_TOTAL_RATED_CALL_AMOUNT,
    MAIN_RATED_TEL_OCM_AMOUNT,
    MAIN_RATED_TEL_MTN_AMOUNT,
    MAIN_RATED_TEL_NEXTTEL_AMOUNT,
    MAIN_RATED_TEL_CAMTEL_AMOUNT,
    MAIN_RATED_TEL_SET_AMOUNT,
    MAIN_RATED_TEL_ROAM_IN_AMOUNT,
    MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
    MAIN_RATED_TEL_SVA_AMOUNT,
    MAIN_RATED_TEL_INT_AMOUNT,
    PROMO_RATED_TEL_OCM_AMOUNT,
    PROMO_RATED_TEL_MTN_AMOUNT,
    PROMO_RATED_TEL_NEXTTEL_AMOUNT,
    PROMO_RATED_TEL_CAMTEL_AMOUNT,
    PROMO_RATED_TEL_SET_AMOUNT,
    PROMO_RATED_TEL_ROAMIN_AMOUNT,
    PROMO_RATED_TEL_ROAMOUT_AMOUNT,
    PROMO_RATED_TEL_INT_AMOUNT,
    OG_SMS_TOTAL_COUNT,
    OG_SMS_OCM_COUNT,
    OG_SMS_MTN_COUNT,
    OG_SMS_NEXTTEL_COUNT,
    OG_SMS_CAMTEL_COUNT,
    OG_SMS_SET_COUNT,
    OG_SMS_ROAM_IN_COUNT,
    OG_SMS_ROAM_OUT_COUNT,
    OG_SMS_SVA_COUNT,
    OG_SMS_INTERNATIONAL_COUNT,
    MAIN_RATED_SMS_AMOUNT,
    MAIN_RATED_SMS_OCM_AMOUNT,
    MAIN_RATED_SMS_MTN_AMOUNT,
    MAIN_RATED_SMS_NEXTTEL_AMOUNT,
    MAIN_RATED_SMS_CAMTEL_AMOUNT,
    MAIN_RATED_SMS_SET_AMOUNT,
    MAIN_RATED_SMS_ROAM_IN_AMOUNT,
    MAIN_RATED_SMS_ROAM_OUT_AMOUNT,
    MAIN_RATED_SMS_SVA_AMOUNT,
    MAIN_RATED_SMS_INT_AMOUNT,
    DATA_MAIN_RATED_AMOUNT,
    DATA_GOS_MAIN_RATED_AMOUNT,
    DATA_PROMO_RATED_AMOUNT,
    DATA_ROAM_MAIN_RATED_AMOUNT,
    DATA_ROAM_PROMO_RATED_AMOUNT,
    DATA_BYTES_RECEIVED,
    DATA_BYTES_SENT,
    DATA_BYTES_USED_IN_BUNDLE,
    DATA_BYTES_USED_PAYGO,
    DATA_BYTES_USED_IN_BUNDLE_ROAM,
    NULL DATA_BYTES_USED_BUNDLE_PAYGO,
    DATA_MMS_USED,
    DATA_MMS_USED_IN_BUNDLE,
    SOS_LOAN_COUNT,
    SOS_LOAN_AMOUNT,
    SOS_REIMBURSEMENT_COUNT,
    SOS_REIMBURSEMENT_AMOUNT,
    SOS_FEES,
    SUBS_BUNDLE_PLENTY_COUNT,
    SUBS_ORANGE_BONUS_COUNT,
    SUBS_FNF_MODIFY_COUNT,
    SUBS_DATA_2G_COUNT,
    SUBS_DATA_3G_JOUR_COUNT,
    SUBS_DATA_3G_SEMAINE_COUNT,
    SUBS_DATA_3G_MOIS_COUNT,
    SUBS_MAXI_BONUS_COUNT,
    SUBS_PRO_COUNT,
    SUBS_INTERN_JOUR_COUNT,
    SUBS_INTERN_SEMAINE_COUNT,
    SUBS_INTERN_MOIS_COUNT,
    SUBS_PACK_SMS_JOUR_COUNT,
    SUBS_PACK_SMS_SEMAINE_COUNT,
    SUBS_PACK_SMS_MOIS_COUNT,
    SUBS_WS_COUNT,
    SUBS_OBONUS_ILLIMITE_COUNT,
    SUBS_ORANGE_PHENIX_COUNT,
    SUBS_RECHARGE_PLENTY_COUNT,
    SUBS_AUTRES_COUNT,
    SUBS_BUNDLE_PLENTY_AMOUNT,
    SUBS_ORANGE_BONUS_AMOUNT,
    SUBS_FNF_MODIFY_AMOUNT,
    SUBS_DATA_2G_AMOUNT,
    SUBS_DATA_3G_JOUR_AMOUNT,
    SUBS_DATA_3G_SEMAINE_AMOUNT,
    SUBS_DATA_3G_MOIS_AMOUNT,
    SUBS_MAXI_BONUS_AMOUNT,
    SUBS_PRO_AMOUNT,
    SUBS_INTERN_JOUR_AMOUNT,
    SUBS_INTERN_SEMAINE_AMOUNT,
    SUBS_INTERN_MOIS_AMOUNT,
    SUBS_PACK_SMS_JOUR_AMOUNT,
    SUBS_PACK_SMS_SEMAINE_AMOUNT,
    SUBS_PACK_SMS_MOIS_AMOUNT,
    SUBS_WS_AMOUNT,
    SUBS_OBONUS_ILLIMITE_AMOUNT,
    SUBS_ORANGE_PHENIX_AMOUNT,
    SUBS_RECHARGE_PLENTY_AMOUNT,
    SUBS_AUTRES_AMOUNT
FROM
    BACKUP_DWH.TT_MSISDN_B2B_FINAL A
    INNER JOIN
    (
        SELECT *
        FROM BACKUP_DWH.FT_MARKETING_DATAMART_FINAL
        WHERE EVENT_DATE = "2020-02-28"
    ) B
ON A.MSISDN=B.MSISDN