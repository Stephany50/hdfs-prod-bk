SELECT 
    MSISDN,
    FORMULE,
    DISCONNEXION_DATE,
    ACTIVATION_DATE,
    LAST_OUTGOING_CALL_MONTH,
    LAST_INCOMING_CALL_MONTH,
    REMAIN_CREDIT_MAIN,
    REMAIN_CREDIT_PROMO,
    PLATFORM_STATUS,
    CONSO_MONTH,
    MAIN_COST_TOTAL,
    MAIN_COST_VOICE_SMS,
    MAIN_COST_DATA,
    MAIN_COST_SUBSCR,
    PROMO_COST_TOTAL,
    PROMO_COST_VOICE_SMS,
    PROMO_COST_DATA,
    PROMO_COST_SUBSCR,
    SITE_NAME ,
    TOWNNAME,
    COMMERCIAL_REGION,
    ADMINISTRATIVE_REGION,
    TAC_CODE_HANDSET,
    CONSTRUCTOR_HANDSET,
    MODEL_HANDSET,
    DATA_COMPATIBLE_HANDSET,
    TAC_CODE_HANDSET_3MBE4,
    CONSTRUCTOR_HANDSET_3MBE4,
    MODEL_HANDSET_3MBE4,
    DATA_COMPATIBLE_HANDSET_3MBE4,
    IMEI_HANDSET,
    DAT2G_COMPATIBLE_HANDSET,
    DAT3G_COMPATIBLE_HANDSET,
    DAT4G_COMPATIBLE_HANDSET,
    IMEI_HANDSET_3MBE4,
    DAT2G_COMPATIBLE_HANDSET_3MBE4,
    DAT3G_COMPATIBLE_HANDSET_3MBE4,
    DAT4G_COMPATIBLE_HANDSET_3MBE4

FROM  MON.SPARK_TT_ANALYSE_DECON_GLOBAL_CONSO1
WHERE ACCOUNT_ACTIVITY_EVENT_DATE = DATE_ADD(Last_day('SLICE_VALUE###'),1)