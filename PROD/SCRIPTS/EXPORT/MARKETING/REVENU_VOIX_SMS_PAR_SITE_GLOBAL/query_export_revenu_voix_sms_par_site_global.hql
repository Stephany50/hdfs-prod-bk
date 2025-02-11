SELECT
     EVENT_MONTH EVENT_MONTH,
     SUM(TOTAL_VOICE_SMS_DOER) TOTAL_VOICE_SMS_DOER,
     SUM(TOTAL_REV_PROVIDER) TOTAL_REV_PROVIDER,
     SUM(TOTAL_SMS_REV_PROVIDER) TOTAL_SMS_REV_PROVIDER,
     SUM(TOTAL_TEL_REV_PROVIDER) TOTAL_TEL_REV_PROVIDER,
     SUM(TOTAL_ONNET_REV_PROVIDER) TOTAL_ONNET_REV_PROVIDER ,
     SUM(TOTAL_OFFNET_REV_PROVIDER) TOTAL_OFFNET_REV_PROVIDER ,
     SUM(TOTAL_OUTROAM_REV_PROVIDER) TOTAL_OUTROAM_REV_PROVIDER,
     SUM(TOTAL_INROAM_REV_PROVIDER) TOTAL_INROAM_REV_PROVIDER ,
     SUM(TOTAL_INTL_REV_PROVIDER) TOTAL_INTL_REV_PROVIDER,
     SUM(MAIN_CONSO) MAIN_CONSO,
     SUM(PROMO_CONSO) PROMO_CONSO,
     SUM(BILLED_SMS_COUNT) BILLED_SMS_COUNT,
     SUM(BILLED_TEL_COUNT) BILLED_TEL_COUNT,
     SUM(BILLED_TEL_DURATION) BILLED_TEL_DURATION,
     SUM(BUNDLE_SMS_COUNT) BUNDLE_SMS_COUNT,
     SUM(BUNDLE_TEL_DURATION) BUNDLE_TEL_DURATION,
     SUM(ONNET_MAIN_CONSO) ONNET_MAIN_CONSO,
     SUM(ONNET_MAIN_TEL_CONSO) ONNET_MAIN_TEL_CONSO ,
     SUM(ONNET_PROMO_TEL_CONSO) ONNET_PROMO_TEL_CONSO,
     SUM(ONNET_TOTAL_CONSO) ONNET_TOTAL_CONSO,
     SUM(ONNET_SMS_CONSO) ONNET_SMS_CONSO,
     SUM(ONNET_SMS_COUNT) ONNET_SMS_COUNT,
     SUM(ONNET_DURATION) ONNET_DURATION ,
     SUM(OFFNET_MAIN_CONSO) OFFNET_MAIN_CONSO,
     SUM(OFFNET_MAIN_TEL_CONSO) OFFNET_MAIN_TEL_CONSO ,
     SUM(OFFNET_PROMO_TEL_CONSO) OFFNET_PROMO_TEL_CONSO,
     SUM(OFFNET_TOTAL_CONSO) OFFNET_TOTAL_CONSO,
     SUM(OFFNET_SMS_CONSO) OFFNET_SMS_CONSO,
     SUM(OFFNET_SMS_COUNT) OFFNET_SMS_COUNT,
     SUM(OFFNET_DURATION) OFFNET_DURATION ,
     SUM(OUTROAM_MAIN_CONSO) OUTROAM_MAIN_CONSO,
     SUM(OUTROAM_MAIN_TEL_CONSO) OUTROAM_MAIN_TEL_CONSO ,
     SUM(OUTROAM_PROMO_TEL_CONSO) OUTROAM_PROMO_TEL_CONSO,
     SUM(OUTROAM_TOTAL_CONSO) OUTROAM_TOTAL_CONSO,
     SUM(OUTROAM_SMS_CONSO) OUTROAM_SMS_CONSO,
     SUM(OUTROAM_SMS_COUNT) OUTROAM_SMS_COUNT,
     SUM(OUTROAM_DURATION) OUTROAM_DURATION ,
     SUM(INROAM_MAIN_CONSO) INROAM_MAIN_CONSO,
     SUM(INROAM_MAIN_TEL_CONSO) INROAM_MAIN_TEL_CONSO ,
     SUM(INROAM_PROMO_TEL_CONSO) INROAM_PROMO_TEL_CONSO,
     SUM(INROAM_TOTAL_CONSO) INROAM_TOTAL_CONSO,
     SUM(INROAM_SMS_CONSO) INROAM_SMS_CONSO,
     SUM(INROAM_SMS_COUNT) INROAM_SMS_COUNT,
     SUM(INROAM_DURATION) INROAM_DURATION ,
     SUM(INTERNATIONAL_MAIN_CONSO) INTERNATIONAL_MAIN_CONSO,
     SUM(INTERNATIONAL_MAIN_TEL_CONSO) INTERNATIONAL_MAIN_TEL_CONSO ,
     SUM(INTERNATIONAL_PROMO_TEL_CONSO) INTERNATIONAL_PROMO_TEL_CONSO ,
     SUM(INTERNATIONAL_TOTAL_CONSO) INTERNATIONAL_TOTAL_CONSO,
     SUM(INTERNATIONAL_SMS_CONSO) INTERNATIONAL_SMS_CONSO ,
     SUM(INTERNATIONAL_SMS_COUNT) INTERNATIONAL_SMS_COUNT ,
     SUM(INTERNATIONAL_DURATION) INTERNATIONAL_DURATION

FROM MON.SPARK_TF_DASHBRD_CMO_VOICESMS_REVM
WHERE EVENT_MONTH >= substr('###SLICE_VALUE###',1,7)
GROUP BY EVENT_MONTH