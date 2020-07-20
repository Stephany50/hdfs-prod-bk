INSERT INTO MON.SPARK_TF_DASHBRD_CMO_VOICESMS_REVM


SELECT
    nvl(b.ADMINISTRATIVE_REGION,'') ADMINISTRATIVE_REGION,
    nvl(b.COMMERCIAL_REGION,'') COMMERCIAL_REGION,
    nvl(b.TOWNNAME,'') TOWNNAME,
    nvl(b.SITE_NAME,'') SITE_NAME,
    nvl(e.CONSTRUCTOR, '') CONSTRUCTOR_HANDSET,
    nvl(e.DATA_COMPATIBLE,'') DATA_COMPATIBLE_HANDSET,
    sum(1) total_voice_sms_doer,
    sum(CASE WHEN MAIN_CALL_COST > 0 THEN 1 ELSE 0 end) total_rev_provider,
    sum(CASE WHEN BILLED_SMS_COUNT > 0 THEN 1 ELSE 0 end) total_sms_rev_provider,
    sum(CASE WHEN BILLED_TEL_COUNT > 0 THEN 1 ELSE 0 end) total_tel_rev_provider,
    sum(CASE WHEN ONNET_MAIN_CONSO > 0 THEN 1 ELSE 0 end) total_onnet_rev_provider,
    sum(CASE WHEN nvl(MTN_MAIN_CONSO,0) + nvl(NEXTTEL_MAIN_CONSO,0) + nvl(CAMTEL_MAIN_CONSO,0) + nvl(SET_MAIN_CONSO,0) > 0 THEN 1 ELSE 0 end) total_offnet_rev_provider,
    sum(CASE WHEN ROAM_MAIN_CONSO > 0 THEN 1 ELSE 0 end) total_outroam_rev_provider,
    sum(CASE WHEN INROAM_MAIN_CONSO > 0 THEN 1 ELSE 0 end) total_inroam_rev_provider,
    sum(CASE WHEN INTERNATIONAL_MAIN_CONSO > 0 THEN 1 ELSE 0 end) total_intl_rev_provider,
    sum(MAIN_CALL_COST) MAIN_CONSO,
    sum(PROMOTIONAL_CALL_COST) PROMO_CONSO,
    sum(BILLED_SMS_COUNT) BILLED_SMS_COUNT,
    sum(BILLED_TEL_COUNT) BILLED_TEL_COUNT,
    sum(BILLED_TEL_DURATION) BILLED_TEL_DURATION,
    sum(BUNDLE_SMS_COUNT) BUNDLE_SMS_COUNT,
    sum(BUNDLE_TEL_DURATION) BUNDLE_TEL_DURATION,
    sum(ONNET_MAIN_CONSO) ONNET_MAIN_CONSO,
    sum(ONNET_MAIN_TEL_CONSO) ONNET_MAIN_TEL_CONSO,
    sum(ONNET_PROMO_TEL_CONSO) ONNET_PROMO_TEL_CONSO,
    sum(ONNET_TOTAL_CONSO) ONNET_TOTAL_CONSO,
    sum(ONNET_SMS_CONSO) ONNET_SMS_CONSO,
    sum(ONNET_SMS_COUNT) ONNET_SMS_COUNT,
    sum(ONNET_DURATION) ONNET_DURATION,
    sum(nvl(MTN_MAIN_CONSO,0) + nvl(NEXTTEL_MAIN_CONSO,0) + nvl(CAMTEL_MAIN_CONSO,0) + nvl(SET_MAIN_CONSO,0)) OFFNET_MAIN_CONSO,
    sum(nvl(MTN_MAIN_TEL_CONSO,0) + nvl(NEXTTEL_MAIN_TEL_CONSO,0) + nvl(CAMTEL_MAIN_TEL_CONSO,0) + nvl(SET_MAIN_TEL_CONSO,0)) OFFNET_MAIN_TEL_CONSO,
    sum(nvl(MTN_PROMO_TEL_CONSO,0) + nvl(NEXTTEL_PROMO_TEL_CONSO,0) + nvl(CAMTEL_PROMO_TEL_CONSO,0) + nvl(SET_PROMO_TEL_CONSO,0)) OFFNET_PROMO_TEL_CONSO,
    sum(nvl(MTN_TOTAL_CONSO,0) + nvl(NEXTTEL_TOTAL_CONSO,0) + nvl(CAMTEL_TOTAL_CONSO,0) + nvl(SET_TOTAL_CONSO,0)) OFFNET_TOTAL_CONSO,
    sum(nvl(MTN_SMS_CONSO,0) + nvl(NEXTTEL_SMS_CONSO,0) + nvl(CAMTEL_SMS_CONSO,0) + nvl(SET_SMS_CONSO,0)) OFFNET_SMS_CONSO,
    sum(nvl(MTN_SMS_COUNT,0) + nvl(NEXTTEL_SMS_COUNT,0) + nvl(CAMTEL_SMS_COUNT,0) + nvl(SET_SMS_COUNT,0)) OFFNET_SMS_COUNT,
    sum(nvl(MTN_DURATION,0) + nvl(NEXTTEL_DURATION,0) + nvl(CAMTEL_DURATION,0) + nvl(SET_DURATION,0)) OFFNET_DURATION,
    sum(ROAM_MAIN_CONSO) OUTROAM_MAIN_CONSO,
    sum(ROAM_MAIN_TEL_CONSO) OUTROAM_MAIN_TEL_CONSO,
    sum(ROAM_PROMO_TEL_CONSO) OUTROAM_PROMO_TEL_CONSO,
    sum(ROAM_TOTAL_CONSO) OUTROAM_TOTAL_CONSO,
    sum(ROAM_SMS_CONSO) OUTROAM_SMS_CONSO,
    sum(ROAM_SMS_COUNT) OUTROAM_SMS_COUNT,
    sum(ROAM_DURATION) OUTROAM_DURATION,
    sum(INROAM_MAIN_CONSO) INROAM_MAIN_CONSO,
    sum(INROAM_MAIN_TEL_CONSO) INROAM_MAIN_TEL_CONSO,
    sum(INROAM_PROMO_TEL_CONSO) INROAM_PROMO_TEL_CONSO,
    sum(INROAM_TOTAL_CONSO) INROAM_TOTAL_CONSO,
    sum(INROAM_SMS_CONSO) INROAM_SMS_CONSO,
    sum(INROAM_SMS_COUNT) INROAM_SMS_COUNT,
    sum(INROAM_DURATION) INROAM_DURATION,
    sum(INTERNATIONAL_MAIN_CONSO) INTERNATIONAL_MAIN_CONSO,
    sum(INTERNATIONAL_MAIN_TEL_CONSO) INTERNATIONAL_MAIN_TEL_CONSO,
    sum(INTERNATIONAL_PROMO_TEL_CONSO) INTERNATIONAL_PROMO_TEL_CONSO,
    sum(INTERNATIONAL_TOTAL_CONSO) INTERNATIONAL_TOTAL_CONSO,
    sum(INTERNATIONAL_SMS_CONSO) INTERNATIONAL_SMS_CONSO,
    sum(INTERNATIONAL_SMS_COUNT) INTERNATIONAL_SMS_COUNT,
    sum(INTERNATIONAL_DURATION) INTERNATIONAL_DURATION,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' EVENT_MONTH




FROM
(
    SELECT
        GG.*,
        e.DATA_COMPATIBLE,
        e.IMEI_RN,
        e.imei,
        e.imsi,
        e.profile_code,
        e.profile_name,
        e.language,
        e.status,
        e.date_first_usage,
        e.date_last_usage,
        e.total_days_count,
        e.activation_date,
        e.smonth,
        e.tac_code,
        e.constructor,
        e.model,
        e.x_phase,
        e.capacity,
        e.wap,
        e.gprs,
        e.market_entry,
        e.ussd_level,
        e.mms,
        e.umts,
        e.color_screen,
        e.port,
        e.camera,
        e.edge,
        e.java,
        e.gallery,
        e.video,
        e.wap_push,
        e.talk_now,
        e.sms_cliquable,
        e.mms_push_class,
        e.gps,
        e.hsdpa,
        e.unik_uma,
        e.insert_refresh_date,
        e.amr,
        e.lte,
        e.bluetooth,
        e.hsupa,
        e.html,
        e.multitouch,
        e.open_os,
        e.videotelephony,
        e.wifi,
        e.technologie,
        e.os,
        e.ref_month,
        e.terminal_type,
        e.tek_radio,
        e.ind,
        e.source


    FROM

    (
        SELECT
            a.*,
            b.SITE_NAME,
            b.TOWNNAME,
            b.ADMINISTRATIVE_REGION,
            b.COMMERCIAL_REGION


        FROM

        (SELECT * FROM MON.SPARK_FT_CONSO_MSISDN_MONTH a WHERE a.EVENT_MONTH = '###SLICE_VALUE###') a

        LEFT JOIN

        (
        select
            MSISDN,
            SITE_NAME,
            TOWNNAME,
            ADMINISTRATIVE_REGION,
            COMMERCIAL_REGION

        from MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION b
        where b.EVENT_MONTH = '###SLICE_VALUE###'
        ) b

        ON a.MSISDN = b.MSISDN

    ) GG

    LEFT JOIN

    (
        SELECT
            substr(e.imei,1,8) tac_code_handset,
            e.*,
            f.*

        FROM
            (
            SELECT
                a.*,
                ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN

            FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a
            WHERE smonth = '###SLICE_VALUE###'
            ) e

            LEFT JOIN

            (
            select
                 a.*,
                (CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or EDGE = 'E' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE

            from dim.dt_handset_ref a
            ) f
            ON substr(e.imei,1,8) = f.tac_code
            WHERE e.IMEI_RN = 1
    ) e

    ON  GG.MSISDN = e.MSISDN

) GGG

GROUP BY
'###SLICE_VALUE###',
nvl(b.ADMINISTRATIVE_REGION,''),
nvl(b.COMMERCIAL_REGION,''),
nvl(b.TOWNNAME,''),
nvl(b.SITE_NAME,''),
nvl(e.CONSTRUCTOR, ''),
nvl(e.DATA_COMPATIBLE,'')