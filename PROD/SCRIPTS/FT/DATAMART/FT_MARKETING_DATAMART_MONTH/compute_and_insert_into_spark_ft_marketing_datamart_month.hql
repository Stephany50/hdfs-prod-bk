INSERT INTO MON.SPARK_FT_MARKETING_DATAMART_MONTH

SELECT

    a.MSISDN,
    CONTRACT_TYPE,
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
    DATA_BYTE_USED_OUT_BUNDLE_ROAM,
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
    SUBS_DATA_FLYBOX_COUNT,
    SUBS_DATA_AUTRES_COUNT,
    SUBS_ROAMING_COUNT,
    SUBS_SMS_AUTRES_COUNT,
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
    SUBS_AUTRES_AMOUNT,
    SUBS_DATA_FLYBOX_AMOUNT,
    SUBS_DATA_AUTRES_AMOUNT,
    SUBS_ROAMING_AMOUNT,
    SUBS_SMS_AUTRES_AMOUNT,
    current_timestamp AS INSERT_DATE,  
    FN_GET_OPERATOR_CODE(a.MSISDN) AS OPERATOR_CODE,
    CASE
    WHEN MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE <= 0 THEN 'O'
    WHEN MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE > 0
    AND MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE <= 500 THEN 'SUPER LOW USERS_1'
    WHEN MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE > 500
    AND MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE <= 1000 THEN 'SUPER LOW USERS_2'
    WHEN MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE > 1000
    AND MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE <= 2000 THEN 'LOW USERS'
    WHEN MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE > 2000
    AND MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE <= 5000 THEN 'MIDDLE USERS'
    WHEN MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE > 5000
    AND MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE <= 10000 THEN 'HIGH USERS'
    ELSE 'SUPER HIGH USERS' -- > 10000
    END  AS SEGMENT,
    NB_JR_ACTIVITE_TOT,
    NB_JR_ACTIVITE_DATA,
    NB_JR_ACTIVITE_VOIX,
    NB_JR_ACTIVITE_INTER,
    NB_JR_ACTIVITE_SMS,
    SUBS_VOICE_COUNT,
    SUBS_SMS_COUNT,
    SUBS_DATA_COUNT,
    SUBS_VOICE_AMOUNT,
    SUBS_SMS_AMOUNT,
    SUBS_DATA_AMOUNT,
    REVENU_MOYEN,
    PREMIUM,
    CONSO_MOY_DATA,
    RECHARGE_MOY,
    PREMIUM_PLUS,
    TER_IMEI,
    EVENT_MONTH

FROM
    (
    SELECT 
        
        SUBSTRING (EVENT_DATE, 1, 7) AS EVENT_MONTH,
        MSISDN,
        CONTRACT_TYPE,
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
        TER_TAC_CODE, TER_IMEI,
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
        REVENU_MOYEN,
        PREMIUM,
        CONSO_MOY_DATA,
        RECHARGE_MOY,
        PREMIUM_PLUS
    
    FROM MON.SPARK_FT_MARKETING_DATAMART
    WHERE EVENT_DATE = last_day('###SLICE_VALUE###'||'-01')
    
    ) a

    FULL JOIN

    (
    SELECT 
        
        MSISDN,
        SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
        SUM(TOTAL_VOICE_REVENUE) AS TOTAL_VOICE_REVENUE,
        SUM(TOTAL_SMS_REVENUE) AS TOTAL_SMS_REVENUE,
        SUM(TOTAL_SUBS_REVENUE) AS TOTAL_SUBS_REVENUE,
        SUM(TOTAL_DATA_REVENUE) AS TOTAL_DATA_REVENUE,
        SUM(ROAM_IN_VOICE_REVENUE) AS ROAM_IN_VOICE_REVENUE,
        SUM(ROAM_OUT_VOICE_REVENUE) AS ROAM_OUT_VOICE_REVENUE,
        SUM(ROAM_IN_SMS_REVENUE) AS ROAM_IN_SMS_REVENUE,
        SUM(ROAM_OUT_SMS_REVENUE) AS ROAM_OUT_SMS_REVENUE,
        SUM(ROAM_DATA_REVENUE) AS ROAM_DATA_REVENUE,
        SUM(C2S_REFILL_COUNT) AS C2S_REFILL_COUNT,
        SUM(C2S_MAIN_REFILL_AMOUNT) AS C2S_MAIN_REFILL_AMOUNT,
        SUM(C2S_PROMO_REFILL_AMOUNT) AS C2S_PROMO_REFILL_AMOUNT,
        SUM(P2P_REFILL_COUNT) AS P2P_REFILL_COUNT,
        SUM(P2P_REFILL_AMOUNT) AS P2P_REFILL_AMOUNT,
        SUM(SCRATCH_REFILL_COUNT) AS SCRATCH_REFILL_COUNT,
        SUM(SCRATCH_MAIN_REFILL_AMOUNT) AS SCRATCH_MAIN_REFILL_AMOUNT,
        SUM(SCRATCH_PROMO_REFILL_AMOUNT) AS SCRATCH_PROMO_REFILL_AMOUNT,
        SUM(P2P_REFILL_FEES) AS P2P_REFILL_FEES,
        SUM(OG_CALL_TOTAL_COUNT) AS OG_CALL_TOTAL_COUNT,
        SUM(OG_CALL_OCM_COUNT) AS OG_CALL_OCM_COUNT,
        SUM(OG_CALL_MTN_COUNT) AS OG_CALL_MTN_COUNT,
        SUM(OG_CALL_NEXTTEL_COUNT) AS OG_CALL_NEXTTEL_COUNT,
        SUM(OG_CALL_CAMTEL_COUNT) AS OG_CALL_CAMTEL_COUNT,
        SUM(OG_CALL_SET_COUNT) AS OG_CALL_SET_COUNT,
        SUM(OG_CALL_ROAM_IN_COUNT) AS OG_CALL_ROAM_IN_COUNT,
        SUM(OG_CALL_ROAM_OUT_COUNT) AS OG_CALL_ROAM_OUT_COUNT,
        SUM(OG_CALL_SVA_COUNT) AS OG_CALL_SVA_COUNT,
        SUM(OG_CALL_INTERNATIONAL_COUNT) AS OG_CALL_INTERNATIONAL_COUNT,
        SUM(OG_RATED_CALL_DURATION) AS OG_RATED_CALL_DURATION,
        SUM(OG_TOTAL_CALL_DURATION) AS OG_TOTAL_CALL_DURATION,
        SUM(RATED_TEL_OCM_DURATION) AS RATED_TEL_OCM_DURATION,
        SUM(RATED_TEL_MTN_DURATION) AS RATED_TEL_MTN_DURATION,
        SUM(RATED_TEL_NEXTTEL_DURATION) AS RATED_TEL_NEXTTEL_DURATION,
        SUM(RATED_TEL_CAMTEL_DURATION) AS RATED_TEL_CAMTEL_DURATION,
        SUM(RATED_TEL_SET_DURATION) AS RATED_TEL_SET_DURATION,
        SUM(RATED_TEL_ROAM_IN_DURATION) AS RATED_TEL_ROAM_IN_DURATION,
        SUM(RATED_TEL_ROAM_OUT_DURATION) AS RATED_TEL_ROAM_OUT_DURATION,
        SUM(RATED_TEL_SVA_DURATION) AS RATED_TEL_SVA_DURATION,
        SUM(RATED_TEL_INT_DURATION) AS RATED_TEL_INT_DURATION,
        SUM(MAIN_RATED_TEL_AMOUNT) AS MAIN_RATED_TEL_AMOUNT,
        SUM(PROMO_RATED_TEL_AMOUNT) AS PROMO_RATED_TEL_AMOUNT,
        SUM(OG_TOTAL_RATED_CALL_AMOUNT) AS OG_TOTAL_RATED_CALL_AMOUNT,
        SUM(MAIN_RATED_TEL_OCM_AMOUNT) AS MAIN_RATED_TEL_OCM_AMOUNT,
        SUM(MAIN_RATED_TEL_MTN_AMOUNT) AS MAIN_RATED_TEL_MTN_AMOUNT,
        SUM(MAIN_RATED_TEL_NEXTTEL_AMOUNT) AS MAIN_RATED_TEL_NEXTTEL_AMOUNT,
        SUM(MAIN_RATED_TEL_CAMTEL_AMOUNT) AS MAIN_RATED_TEL_CAMTEL_AMOUNT,
        SUM(MAIN_RATED_TEL_SET_AMOUNT) AS MAIN_RATED_TEL_SET_AMOUNT,
        SUM(MAIN_RATED_TEL_ROAM_IN_AMOUNT) AS MAIN_RATED_TEL_ROAM_IN_AMOUNT,
        SUM(MAIN_RATED_TEL_ROAM_OUT_AMOUNT) AS MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
        SUM(MAIN_RATED_TEL_SVA_AMOUNT) AS MAIN_RATED_TEL_SVA_AMOUNT,
        SUM(MAIN_RATED_TEL_INT_AMOUNT) AS MAIN_RATED_TEL_INT_AMOUNT,
        SUM(PROMO_RATED_TEL_OCM_AMOUNT) AS PROMO_RATED_TEL_OCM_AMOUNT,
        SUM(PROMO_RATED_TEL_MTN_AMOUNT) AS PROMO_RATED_TEL_MTN_AMOUNT,
        SUM(PROMO_RATED_TEL_NEXTTEL_AMOUNT) AS PROMO_RATED_TEL_NEXTTEL_AMOUNT,
        SUM(PROMO_RATED_TEL_CAMTEL_AMOUNT) AS PROMO_RATED_TEL_CAMTEL_AMOUNT,
        SUM(PROMO_RATED_TEL_SET_AMOUNT) AS PROMO_RATED_TEL_SET_AMOUNT,
        SUM(PROMO_RATED_TEL_ROAMIN_AMOUNT) AS PROMO_RATED_TEL_ROAMIN_AMOUNT,
        SUM(PROMO_RATED_TEL_ROAMOUT_AMOUNT) AS PROMO_RATED_TEL_ROAMOUT_AMOUNT,
        SUM(PROMO_RATED_TEL_INT_AMOUNT) AS PROMO_RATED_TEL_INT_AMOUNT,
        SUM(OG_SMS_TOTAL_COUNT) AS OG_SMS_TOTAL_COUNT,
        SUM(OG_SMS_OCM_COUNT) AS OG_SMS_OCM_COUNT,
        SUM(OG_SMS_MTN_COUNT) AS OG_SMS_MTN_COUNT,
        SUM(OG_SMS_NEXTTEL_COUNT) AS OG_SMS_NEXTTEL_COUNT,
        SUM(OG_SMS_CAMTEL_COUNT) AS OG_SMS_CAMTEL_COUNT,
        SUM(OG_SMS_SET_COUNT) AS OG_SMS_SET_COUNT,
        SUM(OG_SMS_ROAM_IN_COUNT) AS OG_SMS_ROAM_IN_COUNT,
        SUM(OG_SMS_ROAM_OUT_COUNT) AS OG_SMS_ROAM_OUT_COUNT,
        SUM(OG_SMS_SVA_COUNT) AS OG_SMS_SVA_COUNT,
        SUM(OG_SMS_INTERNATIONAL_COUNT) AS OG_SMS_INTERNATIONAL_COUNT,
        SUM(MAIN_RATED_SMS_AMOUNT) AS MAIN_RATED_SMS_AMOUNT,
        SUM(MAIN_RATED_SMS_OCM_AMOUNT) AS MAIN_RATED_SMS_OCM_AMOUNT,
        SUM(MAIN_RATED_SMS_MTN_AMOUNT) AS MAIN_RATED_SMS_MTN_AMOUNT,
        SUM(MAIN_RATED_SMS_NEXTTEL_AMOUNT) AS MAIN_RATED_SMS_NEXTTEL_AMOUNT,
        SUM(MAIN_RATED_SMS_CAMTEL_AMOUNT) AS MAIN_RATED_SMS_CAMTEL_AMOUNT,
        SUM(MAIN_RATED_SMS_SET_AMOUNT) AS MAIN_RATED_SMS_SET_AMOUNT,
        SUM(MAIN_RATED_SMS_ROAM_IN_AMOUNT) AS MAIN_RATED_SMS_ROAM_IN_AMOUNT,
        SUM(MAIN_RATED_SMS_ROAM_OUT_AMOUNT) AS MAIN_RATED_SMS_ROAM_OUT_AMOUNT,
        SUM(MAIN_RATED_SMS_SVA_AMOUNT) AS MAIN_RATED_SMS_SVA_AMOUNT,
        SUM(MAIN_RATED_SMS_INT_AMOUNT) AS MAIN_RATED_SMS_INT_AMOUNT,
        SUM(DATA_MAIN_RATED_AMOUNT) AS DATA_MAIN_RATED_AMOUNT,
        SUM(DATA_GOS_MAIN_RATED_AMOUNT) AS DATA_GOS_MAIN_RATED_AMOUNT,
        SUM(DATA_PROMO_RATED_AMOUNT) AS DATA_PROMO_RATED_AMOUNT,
        SUM(DATA_ROAM_MAIN_RATED_AMOUNT) AS DATA_ROAM_MAIN_RATED_AMOUNT,
        SUM(DATA_ROAM_PROMO_RATED_AMOUNT) AS DATA_ROAM_PROMO_RATED_AMOUNT,
        SUM(DATA_BYTES_RECEIVED) AS DATA_BYTES_RECEIVED,
        SUM(DATA_BYTES_SENT) AS DATA_BYTES_SENT,
        SUM(DATA_BYTES_USED_IN_BUNDLE) AS DATA_BYTES_USED_IN_BUNDLE,
        SUM(DATA_BYTES_USED_PAYGO) AS DATA_BYTES_USED_PAYGO,
        SUM(DATA_BYTES_USED_IN_BUNDLE_ROAM) AS DATA_BYTES_USED_IN_BUNDLE_ROAM,
        SUM(DATA_BYTE_USED_OUT_BUNDLE_ROAM) AS DATA_BYTE_USED_OUT_BUNDLE_ROAM,
        SUM(DATA_MMS_USED) AS DATA_MMS_USED,
        SUM(DATA_MMS_USED_IN_BUNDLE) AS DATA_MMS_USED_IN_BUNDLE,
        SUM(SOS_LOAN_COUNT) AS SOS_LOAN_COUNT,
        SUM(SOS_LOAN_AMOUNT) AS SOS_LOAN_AMOUNT,
        SUM(SOS_REIMBURSEMENT_COUNT) AS SOS_REIMBURSEMENT_COUNT,
        SUM(SOS_REIMBURSEMENT_AMOUNT) AS SOS_REIMBURSEMENT_AMOUNT,
        SUM(SOS_FEES) AS SOS_FEES,
        SUM(SUBS_BUNDLE_PLENTY_COUNT) AS SUBS_BUNDLE_PLENTY_COUNT,
        SUM(SUBS_ORANGE_BONUS_COUNT) AS SUBS_ORANGE_BONUS_COUNT,
        SUM(SUBS_FNF_MODIFY_COUNT) AS SUBS_FNF_MODIFY_COUNT,
        SUM(SUBS_DATA_2G_COUNT) AS SUBS_DATA_2G_COUNT,
        SUM(SUBS_DATA_3G_JOUR_COUNT) AS SUBS_DATA_3G_JOUR_COUNT,
        SUM(SUBS_DATA_3G_SEMAINE_COUNT) AS SUBS_DATA_3G_SEMAINE_COUNT,
        SUM(SUBS_DATA_3G_MOIS_COUNT) AS SUBS_DATA_3G_MOIS_COUNT,
        SUM(SUBS_MAXI_BONUS_COUNT) AS SUBS_MAXI_BONUS_COUNT,
        SUM(SUBS_PRO_COUNT) AS SUBS_PRO_COUNT,
        SUM(SUBS_INTERN_JOUR_COUNT) AS SUBS_INTERN_JOUR_COUNT,
        SUM(SUBS_INTERN_SEMAINE_COUNT) AS SUBS_INTERN_SEMAINE_COUNT,
        SUM(SUBS_INTERN_MOIS_COUNT) AS SUBS_INTERN_MOIS_COUNT,
        SUM(SUBS_PACK_SMS_JOUR_COUNT) AS SUBS_PACK_SMS_JOUR_COUNT,
        SUM(SUBS_PACK_SMS_SEMAINE_COUNT) AS SUBS_PACK_SMS_SEMAINE_COUNT,
        SUM(SUBS_PACK_SMS_MOIS_COUNT) AS SUBS_PACK_SMS_MOIS_COUNT,
        SUM(SUBS_WS_COUNT) AS SUBS_WS_COUNT,
        SUM(SUBS_OBONUS_ILLIMITE_COUNT) AS SUBS_OBONUS_ILLIMITE_COUNT,
        SUM(SUBS_ORANGE_PHENIX_COUNT) AS SUBS_ORANGE_PHENIX_COUNT,
        SUM(SUBS_RECHARGE_PLENTY_COUNT) AS SUBS_RECHARGE_PLENTY_COUNT,
        SUM(SUBS_AUTRES_COUNT) AS SUBS_AUTRES_COUNT,
        SUM(SUBS_DATA_FLYBOX_COUNT) AS SUBS_DATA_FLYBOX_COUNT,
        SUM(SUBS_DATA_AUTRES_COUNT) AS SUBS_DATA_AUTRES_COUNT,
        SUM(SUBS_ROAMING_COUNT) AS SUBS_ROAMING_COUNT,
        SUM(SUBS_SMS_AUTRES_COUNT) AS SUBS_SMS_AUTRES_COUNT,
        SUM(SUBS_BUNDLE_PLENTY_AMOUNT) AS SUBS_BUNDLE_PLENTY_AMOUNT,
        SUM(SUBS_ORANGE_BONUS_AMOUNT) AS SUBS_ORANGE_BONUS_AMOUNT,
        SUM(SUBS_FNF_MODIFY_AMOUNT) AS SUBS_FNF_MODIFY_AMOUNT,
        SUM(SUBS_DATA_2G_AMOUNT) AS SUBS_DATA_2G_AMOUNT,
        SUM(SUBS_DATA_3G_JOUR_AMOUNT) AS SUBS_DATA_3G_JOUR_AMOUNT,
        SUM(SUBS_DATA_3G_SEMAINE_AMOUNT) AS SUBS_DATA_3G_SEMAINE_AMOUNT,
        SUM(SUBS_DATA_3G_MOIS_AMOUNT) AS SUBS_DATA_3G_MOIS_AMOUNT,
        SUM(SUBS_MAXI_BONUS_AMOUNT) AS SUBS_MAXI_BONUS_AMOUNT,
        SUM(SUBS_PRO_AMOUNT) AS SUBS_PRO_AMOUNT,
        SUM(SUBS_INTERN_JOUR_AMOUNT) AS SUBS_INTERN_JOUR_AMOUNT,
        SUM(SUBS_INTERN_SEMAINE_AMOUNT) AS SUBS_INTERN_SEMAINE_AMOUNT,
        SUM(SUBS_INTERN_MOIS_AMOUNT) AS SUBS_INTERN_MOIS_AMOUNT,
        SUM(SUBS_PACK_SMS_JOUR_AMOUNT) AS SUBS_PACK_SMS_JOUR_AMOUNT,
        SUM(SUBS_PACK_SMS_SEMAINE_AMOUNT) AS SUBS_PACK_SMS_SEMAINE_AMOUNT,
        SUM(SUBS_PACK_SMS_MOIS_AMOUNT) AS SUBS_PACK_SMS_MOIS_AMOUNT,
        SUM(SUBS_WS_AMOUNT) AS SUBS_WS_AMOUNT,
        SUM(SUBS_OBONUS_ILLIMITE_AMOUNT) AS SUBS_OBONUS_ILLIMITE_AMOUNT,
        SUM(SUBS_ORANGE_PHENIX_AMOUNT) AS SUBS_ORANGE_PHENIX_AMOUNT,
        SUM(SUBS_RECHARGE_PLENTY_AMOUNT) AS SUBS_RECHARGE_PLENTY_AMOUNT,
        SUM(SUBS_AUTRES_AMOUNT) AS SUBS_AUTRES_AMOUNT,
        SUM(SUBS_DATA_FLYBOX_AMOUNT) AS SUBS_DATA_FLYBOX_AMOUNT,
        SUM(SUBS_DATA_AUTRES_AMOUNT) AS SUBS_DATA_AUTRES_AMOUNT,
        SUM(SUBS_ROAMING_AMOUNT) AS SUBS_ROAMING_AMOUNT,
        SUM(SUBS_SMS_AUTRES_AMOUNT) AS SUBS_SMS_AUTRES_AMOUNT,
        SUM(CASE WHEN OG_CALL_TOTAL_COUNT > 0 OR OG_SMS_TOTAL_COUNT > 0 OR (DATA_BYTES_RECEIVED + DATA_BYTES_SENT) > 0 THEN 1 ELSE 0 END) AS NB_JR_ACTIVITE_TOT,
        SUM(CASE WHEN (DATA_BYTES_RECEIVED + DATA_BYTES_SENT) > 0 THEN 1 ELSE 0 END) AS NB_JR_ACTIVITE_DATA,
        SUM(CASE WHEN OG_CALL_OCM_COUNT + OG_CALL_MTN_COUNT + OG_CALL_NEXTTEL_COUNT + OG_CALL_CAMTEL_COUNT + OG_CALL_SET_COUNT > 0 THEN 1 ELSE 0 END) AS NB_JR_ACTIVITE_VOIX,
        SUM(CASE WHEN OG_CALL_INTERNATIONAL_COUNT > 0 THEN 1 ELSE 0 END) AS NB_JR_ACTIVITE_INTER,
        SUM(CASE WHEN OG_SMS_TOTAL_COUNT > 0 THEN 1 ELSE 0 END) AS NB_JR_ACTIVITE_SMS,
        SUM(SUBS_VOICE_COUNT) AS SUBS_VOICE_COUNT,
        SUM(SUBS_SMS_COUNT) AS SUBS_SMS_COUNT,
        SUM(SUBS_VOICE_AMOUNT) AS SUBS_DATA_COUNT,
        SUM(SUBS_VOICE_AMOUNT) AS SUBS_VOICE_AMOUNT,
        SUM(SUBS_SMS_AMOUNT) AS SUBS_SMS_AMOUNT,
        SUM(SUBS_DATA_AMOUNT) AS SUBS_DATA_AMOUNT
        
    FROM MON.SPARK_FT_MARKETING_DATAMART
    WHERE EVENT_DATE BETWEEN TO_DATE('###SLICE_VALUE###'||'-01') AND LAST_DAY('###SLICE_VALUE###'||'-01')
    GROUP BY MSISDN
    ) b
    ON a.MSISDN = b.MSISDN
    
WHERE FN_GET_NNP_MSISDN_SIMPLE_DESTN(a.MSISDN) IN ('SET', 'OCM','MTN','VIETTEL')
