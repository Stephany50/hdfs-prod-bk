INSERT INTO MON.SPARK_FT_CONSO_MSISDN_DAY
( MSISDN, FORMULE, CONSO, SMS_COUNT, TEL_COUNT, TEL_DURATION, CONSO_TEL_MAIN, BILLED_SMS_COUNT, BILLED_TEL_COUNT, BILLED_TEL_DURATION, CONSO_SMS, CONSO_TEL, PROMOTIONAL_CALL_COST, MAIN_CALL_COST ,SRC_TABLE, others_vas_total_count, others_vas_duration, others_vas_main_cost, others_vas_promo_cost, national_total_count, national_sms_count, national_duration , national_main_cost, national_promo_cost, national_sms_main_cost, national_sms_promo_cost, mtn_total_count, mtn_sms_count, mtn_duration, mtn_total_conso, mtn_sms_conso, camtel_total_count, camtel_sms_count, camtel_duration, camtel_total_conso, camtel_sms_conso , international_total_count, international_sms_count, international_duration, international_total_conso, international_sms_conso , ONNET_SMS_COUNT, ONNET_DURATION, ONNET_TOTAL_CONSO,    ONNET_MAIN_CONSO, ONNET_MAIN_TEL_CONSO, ONNET_PROMO_TEL_CONSO, ONNET_SMS_CONSO,MTN_MAIN_CONSO , CAMTEL_MAIN_CONSO, INTERNATIONAL_MAIN_CONSO, ROAM_TOTAL_COUNT, ROAM_SMS_COUNT, ROAM_DURATION, ROAM_TOTAL_CONSO, ROAM_MAIN_CONSO, ROAM_SMS_CONSO, SET_TOTAL_COUNT, SET_SMS_COUNT, SET_DURATION, SET_TOTAL_CONSO, SET_SMS_CONSO, SET_MAIN_CONSO , INROAM_TOTAL_COUNT, INROAM_SMS_COUNT, INROAM_DURATION, INROAM_TOTAL_CONSO, INROAM_MAIN_CONSO, INROAM_SMS_CONSO, NEXTTEL_TOTAL_COUNT, NEXTTEL_SMS_COUNT, NEXTTEL_DURATION, NEXTTEL_TOTAL_CONSO, NEXTTEL_SMS_CONSO, NEXTTEL_MAIN_CONSO , BUNDLE_SMS_COUNT, BUNDLE_TEL_DURATION, SET_MAIN_TEL_CONSO,MTN_MAIN_TEL_CONSO, NEXTTEL_MAIN_TEL_CONSO, CAMTEL_MAIN_TEL_CONSO, INTERNATIONAL_MAIN_TEL_CONSO, ROAM_MAIN_TEL_CONSO, INROAM_MAIN_TEL_CONSO,SET_PROMO_TEL_CONSO, MTN_PROMO_TEL_CONSO, NEXTTEL_PROMO_TEL_CONSO, CAMTEL_PROMO_TEL_CONSO, INTERNATIONAL_PROMO_TEL_CONSO, ROAM_PROMO_TEL_CONSO, INROAM_PROMO_TEL_CONSO, ONNET_BILLED_TEL_DURATION,SET_BILLED_TEL_DURATION,NEXTTEL_BILLED_TEL_DURATION,MTN_BILLED_TEL_DURATION,    CAMTEL_BILLED_TEL_DURATION,INTERNATIONAL_BIL_TEL_DURATION,ROAM_BILLED_TEL_DURATION,INROAM_BILLED_TEL_DURATION,  ONNET_BILLED_TEL_COUNT,SET_BILLED_TEL_COUNT,NEXTTEL_BILLED_TEL_COUNT,MTN_BILLED_TEL_COUNT,CAMTEL_BILLED_TEL_COUNT,  INTERNATIONAL_BILLED_TEL_COUNT,ROAM_BILLED_TEL_COUNT,INROAM_BILLED_TEL_COUNT,ONNET_TEL_COUNT,SET_TEL_COUNT,  NEXTTEL_TEL_COUNT,MTN_TEL_COUNT,CAMTEL_TEL_COUNT,INTERNATIONAL_TEL_COUNT,ROAM_TEL_COUNT,INROAM_TEL_COUNT,SVA_COUNT,SVA_DURATION,SVA_MAIN_CONSO,SVA_PROMO_CONSO,SVA_TEL_COUNT,SVA_BILLED_DURATION,SVA_BILLED_TEL_CONSO,SVA_SMS_COUNT,SVA_SMS_CONSO, OPERATOR_CODE, INSERT_DATE, EVENT_DATE)

SELECT

    'IT_CRA_ICC_TRADUIT' SRC_TABLE
     , a.CHARGED_PARTY  MSISDN
     , MAX (a.COMMERCIAL_PROFILE) FORMULE
     , SUM ((PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT)) CONSO
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'NVX_SMS' THEN  EVENT_COUNT  ELSE  0    END ) SMS_COUNT
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' THEN  EVENT_COUNT  ELSE  0    END ) TEL_COUNT
     , SUM (DURATION) TEL_DURATION
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' THEN  MAIN_RATED_AMOUNT  ELSE  0    END ) AS CONSO_TEL_MAIN
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'NVX_SMS' AND (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT) > 0 THEN  EVENT_COUNT   ELSE  0    END ) BILLED_SMS_COUNT
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' AND (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT) > 0 THEN   EVENT_COUNT   ELSE  0    END ) BILLED_TEL_COUNT
     , SUM (CASE    WHEN  (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT) > 0 THEN  DURATION   ELSE  0    END) BILLED_TEL_DURATION
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'NVX_SMS' THEN  (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT)   ELSE  0    END ) CONSO_SMS
     , SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' THEN  (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT)   ELSE  0    END ) CONSO_TEL
     , SUM (PROMO_RATED_AMOUNT) PROMOTIONAL_CALL_COST
     , SUM (MAIN_RATED_AMOUNT) MAIN_CALL_COST
     , SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN EVENT_COUNT ELSE 0 END ) others_vas_total_count
     , SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN DURATION ELSE 0 END ) others_vas_duration
     , SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN MAIN_RATED_AMOUNT ELSE 0 END ) others_vas_main_cost
     , SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN PROMO_RATED_AMOUNT ELSE 0 END ) others_vas_promo_cost
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN EVENT_COUNT ELSE 0 END ) national_total_count
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) national_sms_count
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN DURATION ELSE 0 END ) national_duration
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN MAIN_RATED_AMOUNT ELSE 0 END ) national_main_cost
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN PROMO_RATED_AMOUNT ELSE 0 END ) national_promo_cost
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN MAIN_RATED_AMOUNT ELSE 0 END ) national_sms_main_cost
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN PROMO_RATED_AMOUNT ELSE 0 END ) national_sms_promo_cost
     --, SUM (CASE WHEN DEST_OPERATOR IN ('MTN') THEN EVENT_COUNT ELSE 0 END ) mtn_total_count
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN EVENT_COUNT ELSE 0 END ) mtn_total_count
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) mtn_sms_count
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN DURATION ELSE 0 END ) mtn_duration
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) mtn_total_conso
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) mtn_sms_conso
     --, SUM (CASE WHEN DEST_OPERATOR IN ('CAMTEL_FIX', 'CTPHONE') THEN EVENT_COUNT ELSE 0 END ) camtel_total_count
     , SUM (CASE WHEN DEST_OPERATOR = 'OUT_NAT_MOB_CAM' THEN EVENT_COUNT ELSE 0 END ) camtel_total_count
     , SUM (CASE WHEN DEST_OPERATOR = 'OUT_NAT_MOB_CAM' AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) camtel_sms_count
     , SUM (CASE WHEN DEST_OPERATOR = 'OUT_NAT_MOB_CAM' THEN DURATION ELSE 0 END ) camtel_duration
     , SUM (CASE WHEN DEST_OPERATOR = 'OUT_NAT_MOB_CAM' THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) camtel_total_conso
     , SUM (CASE WHEN DEST_OPERATOR = 'OUT_NAT_MOB_CAM' AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) camtel_sms_conso
     , SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN EVENT_COUNT ELSE 0 END ) international_total_count
     , SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO', 'OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) international_sms_count
     , SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN DURATION ELSE 0 END ) international_duration
     , SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) international_total_conso
     , SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) international_sms_conso
     --ONNET
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN 1 ELSE 0 END ) ONNET_SMS_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN DURATION ELSE 0 END ) ONNET_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) ONNET_TOTAL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) ONNET_MAIN_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) ONNET_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN PROMO_RATED_AMOUNT ELSE 0 END ) ONNET_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) ONNET_SMS_CONSO
     -- OFFNET ADDS
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN MAIN_RATED_AMOUNT ELSE 0 END ) MTN_MAIN_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) CAMTEL_MAIN_CONSO
     , SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT') THEN MAIN_RATED_AMOUNT ELSE 0 END ) INTERNATIONAL_MAIN_CONSO
     -- ROAMING OUT
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_ROAM_MO') THEN EVENT_COUNT ELSE 0 END ) ROAM_TOTAL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN 1 ELSE 0 END ) ROAM_SMS_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN DURATION ELSE 0 END ) ROAM_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) ROAM_TOTAL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN MAIN_RATED_AMOUNT ELSE 0 END ) ROAM_MAIN_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN MAIN_RATED_AMOUNT ELSE 0 END ) ROAM_SMS_CONSO
     -- MVNO
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN EVENT_COUNT ELSE 0 END ) SET_TOTAL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN 1 ELSE 0 END ) SET_SMS_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN DURATION ELSE 0 END ) SET_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) SET_TOTAL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN MAIN_RATED_AMOUNT ELSE 0 END ) SET_MAIN_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN MAIN_RATED_AMOUNT ELSE 0 END ) SET_SMS_CONSO
     -- ROAMING IN
     , SUM (CASE WHEN DEST_OPERATOR IN ('IN_ROAM_MT') THEN EVENT_COUNT ELSE 0 END ) INROAM_TOTAL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN 1 ELSE 0 END ) INROAM_SMS_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN DURATION ELSE 0 END ) INROAM_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) INROAM_TOTAL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN MAIN_RATED_AMOUNT ELSE 0 END ) INROAM_MAIN_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN MAIN_RATED_AMOUNT ELSE 0 END ) INROAM_SMS_CONSO
     -- NEXTTEL
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN EVENT_COUNT ELSE 0 END ) NEXTTEL_TOTAL_COUNT
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) nexttel_sms_count
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN DURATION ELSE 0 END ) nexttel_duration
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) nexttel_total_conso
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ) nexttel_sms_conso
     , SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN MAIN_RATED_AMOUNT ELSE 0 END ) NEXTTEL_MAIN_CONSO
     , SUM (CASE WHEN  a.SERVICE_CODE = 'NVX_SMS' THEN BUNDLE_SMS_USED_VOLUME  ELSE  0 END ) BUNDLE_SMS_COUNT
     , SUM (CASE WHEN  a.SERVICE_CODE = 'VOI_VOX' THEN BUNDLE_TIME_USED_VOLUME  ELSE  0 END ) BUNDLE_TEL_DURATION
     -- Calcul des nouveaux champs 20150526 par GPE: guy.penda@orange.com
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) SET_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) MTN_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) NEXTTEL_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) CAMTEL_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) INTERNATIONAL_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) ROAM_MAIN_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) INROAM_MAIN_TEL_CONSO
     --
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) SET_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) MTN_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) NEXTTEL_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) CAMTEL_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) INTERNATIONAL_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) ROAM_PROMO_TEL_CONSO
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN NVL(PROMO_RATED_AMOUNT,0) ELSE 0 END ) INROAM_PROMO_TEL_CONSO
     -- Fin calcul nouveaux champs 20150526
     -- Calcul des nouveaux champs 20151019 par GPE: dimitri.happi@orange.com
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN RATED_DURATION ELSE 0 END ) ONNET_BILLED_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN RATED_DURATION ELSE 0 END ) SET_BILLED_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN RATED_DURATION ELSE 0 END ) NEXTTEL_BILLED_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN RATED_DURATION ELSE 0 END ) MTN_BILLED_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN RATED_DURATION ELSE 0 END ) CAMTEL_BILLED_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN RATED_DURATION ELSE 0 END ) INTERNATIONAL_BIL_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN RATED_DURATION ELSE 0 END ) ROAM_BILLED_TEL_DURATION
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN RATED_DURATION ELSE 0 END ) INROAM_BILLED_TEL_DURATION
     --
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN RATED_EVENT_COUNT ELSE 0 END ) ONNET_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN RATED_EVENT_COUNT ELSE 0 END ) SET_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN RATED_EVENT_COUNT ELSE 0 END ) NEXTTEL_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN RATED_EVENT_COUNT ELSE 0 END ) MTN_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN RATED_EVENT_COUNT ELSE 0 END ) CAMTEL_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN RATED_EVENT_COUNT ELSE 0 END ) INTERNATIONAL_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN RATED_EVENT_COUNT ELSE 0 END ) ROAM_BILLED_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN RATED_EVENT_COUNT ELSE 0 END ) INROAM_BILLED_TEL_COUNT
     --
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN EVENT_COUNT ELSE 0 END ) ONNET_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN EVENT_COUNT ELSE 0 END ) SET_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN EVENT_COUNT ELSE 0 END ) NEXTTEL_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN EVENT_COUNT ELSE 0 END ) MTN_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN EVENT_COUNT ELSE 0 END ) CAMTEL_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN EVENT_COUNT ELSE 0 END ) INTERNATIONAL_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN EVENT_COUNT ELSE 0 END ) ROAM_TEL_COUNT
     , SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN EVENT_COUNT ELSE 0 END ) INROAM_TEL_COUNT,

    SUM (CASE WHEN DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN EVENT_COUNT ELSE 0 END ) SVA_COUNT,
    SUM (CASE WHEN DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN DURATION ELSE 0 END ) SVA_DURATION,
    SUM (CASE WHEN DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ) SVA_MAIN_CONSO,
    SUM (CASE WHEN DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN PROMO_RATED_AMOUNT ELSE 0 END ) SVA_PROMO_CONSO,
    SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN EVENT_COUNT ELSE 0 END ) SVA_TEL_COUNT,
    SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') AND (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT) > 0 THEN DURATION ELSE 0 END ) SVA_BILLED_DURATION,
    SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ) SVA_BILLED_TEL_CONSO,
    SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA')  THEN EVENT_COUNT ELSE 0 END ) SVA_SMS_COUNT,
    SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ) SVA_SMS_CONSO
     , a.OPERATOR_CODE
     ,current_timestamp INSERT_DATE
     ,"###SLICE_VALUE###" EVENT_DATE
FROM
    (
        SELECT

            a.CHARGED_PARTY
             , a.OPERATOR_CODE
             , (CASE
                    WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                    WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                    WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                    WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                    WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                    WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                    WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                    WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                    WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                    WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                    WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                    WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                    ELSE 'AUT'
            END ) SERVICE_CODE
             , ( CASE WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
                      WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                      WHEN a.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
                      WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                      WHEN a.Call_Destination_Code IN ('NEXTTEL','NEXTTEL_D') THEN 'OUT_NAT_MOB_NEX' --NEXTTEL
                      WHEN a.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
                      WHEN a.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
                      WHEN a.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
                      WHEN a.Call_Destination_Code = 'TCRMG' THEN 'IN_ROAM_MT'
                      WHEN a.Call_Destination_Code = 'INT' THEN 'OUT_INT'
                      WHEN a.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
                      ELSE Call_Destination_Code END ) DEST_OPERATOR
             , sum(PROMO_RATED_AMOUNT) PROMO_RATED_AMOUNT
             , sum(MAIN_RATED_AMOUNT) MAIN_RATED_AMOUNT
             , sum(CALL_PROCESS_TOTAL_DURATION) DURATION
             , SUM(CASE WHEN a.Main_Rated_Amount + a.Promo_Rated_Amount > 0 THEN CALL_PROCESS_TOTAL_DURATION ELSE 0 END) AS RATED_DURATION
             , SUM(CASE WHEN a.Main_Rated_Amount + a.Promo_Rated_Amount > 0 THEN 1 ELSE 0 END) AS RATED_EVENT_COUNT
             , SUM (1) EVENT_COUNT
             ,  MAX (a.COMMERCIAL_PROFILE) COMMERCIAL_PROFILE
             , SUM(BUNDLE_SMS_USED_VOLUME) BUNDLE_SMS_USED_VOLUME
             , SUM(BUNDLE_TIME_USED_VOLUME) BUNDLE_TIME_USED_VOLUME
        FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID a
        WHERE
                TRANSACTION_DATE ="###SLICE_VALUE###"
          --AND Rated_Volume_list NOT LIKE '%-%'
            /*AND ( NVL (Rated_Volume, 0) > 0 OR  NVL (RATED_AMOUNT_IN_BUNDLE, 0) > 0 AND NVL (RAW_EVENT_COST, 0) > 0
                        OR BILLING_TERM_INDICATOR = 0  -- OR NETWORK_TERM_INDICATOR = 0
                      )*/
          AND Main_Rated_Amount >= 0
          AND Promo_Rated_Amount >= 0
--                            AND BUNDLE_SMS_USED_VOLUME >= 0
--                            AND BUNDLE_TIME_USED_VOLUME >= 0
        GROUP BY a.CHARGED_PARTY, a.OPERATOR_CODE
               , (CASE
                      WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                      WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                      WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                      WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                      WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                      WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                      WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                      WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                      WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                      WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                      WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                      WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                      ELSE 'AUT'
            END )
               --, mon.fn_interco_destination (OTHER_PARTY)
               , ( CASE WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
                        WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                        WHEN a.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
                        WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                        WHEN a.Call_Destination_Code IN ('NEXTTEL','NEXTTEL_D') THEN 'OUT_NAT_MOB_NEX' --NEXTTEL
                        WHEN a.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
                        WHEN a.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
                        WHEN a.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
                        WHEN a.Call_Destination_Code = 'TCRMG' THEN 'IN_ROAM_MT'
                        WHEN a.Call_Destination_Code = 'INT' THEN 'OUT_INT'
                        WHEN a.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
                        ELSE Call_Destination_Code END )
    ) a
GROUP BY
    a.CHARGED_PARTY
       , a.OPERATOR_CODE
;