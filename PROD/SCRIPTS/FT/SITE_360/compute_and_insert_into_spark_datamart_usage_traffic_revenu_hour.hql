INSERT INTO MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
SELECT
    A.MSISDN MSISDN,
    A.HOUR_PERIOD EVENT_HOUR,
    CASE
        WHEN TRIM(C.TEK_RADIO) IN ('2G', 'GSM') THEN '2G'
        WHEN TRIM(C.TEK_RADIO) IN ('GPRS') THEN '2.5G'
        WHEN TRIM(C.TEK_RADIO) IN ('EDGE') THEN '2.75G'
        WHEN TRIM(C.TEK_RADIO) IN ('3G', 'HSDPA', '3G EDGE', 'HSPA', 'HSPA+', 'HSUPA', 'UMTS') THEN '3G'
        WHEN TRIM(C.TEK_RADIO) IN ('LTE CA', 'LTE') THEN '4G'
        WHEN TRIM(C.TEK_RADIO) IN ('5G') THEN '5G'
    ELSE 'INCONNU' END -- C.TEK_RADIO = '5G' 
    TECHNO_DEVICE,
    TRAFIC_VOIX, 
    TRAFIC_DATA, 
    TRAFIC_SMS, 
    REVENU_VOIX_PYG,
    REVENU_VOIX_SUBS, -- en cours
    REVENU_DATA, -- en cours
    REVENU_SMS_PYG,
    REVENU_SMS_SUBS, -- en cours
    SITE_NAME,
    TOWN,
    REGION,
    COMMERCIAL_REGION,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        MSISDN,
        HOUR_PERIOD,
        SUM(TRAFIC_VOIX) TRAFIC_VOIX,
        SUM(TRAFIC_DATA) TRAFIC_DATA,
        SUM(TRAFIC_SMS) TRAFIC_SMS,
        SUM(REVENU_VOIX_PYG) REVENU_VOIX_PYG,
        SUM(REVENU_VOIX_SUBS) REVENU_VOIX_SUBS,
        SUM(REVENU_DATA) REVENU_DATA,
        SUM(REVENU_SMS_PYG) REVENU_SMS_PYG,
        SUM(REVENU_SMS_SUBS) REVENU_SMS_SUBS
    FROM
    (
        -- Revenu paygo voice/sms et traffic voice/sms/data
        SELECT
            MSISDN,
            HOUR_PERIOD,
            SUM(TRAFIC_VOIX) TRAFIC_VOIX,
            SUM(TRAFIC_DATA) TRAFIC_DATA,
            SUM(TRAFIC_SMS) TRAFIC_SMS,
            SUM(REVENU_VOIX_PYG) REVENU_VOIX_PYG,
            SUM(0) REVENU_VOIX_SUBS,
            SUM(0) REVENU_DATA,
            SUM(REVENU_SMS_PYG) REVENU_SMS_PYG,
            SUM(0) REVENU_SMS_SUBS
        FROM 
        (
            SELECT 
                CHARGED_PARTY MSISDN,
                SUBSTR(TRANSACTION_TIME, 1, 2) HOUR_PERIOD,
                NVL(SUM(CALL_PROCESS_TOTAL_DURATION/60), 0) TRAFIC_VOIX, 
                SUM(0) TRAFIC_DATA,
                NVL(SUM(
                    CASE 
                        WHEN 
                            (
                                CASE
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
                                END
                            ) IN ('NVX_SMS')
                            AND 
                            (
                                CASE 
                                WHEN Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
                                WHEN Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                                WHEN Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
                                WHEN Call_Destination_Code IN ('NEXTTEL','NEXTTEL_D') THEN 'OUT_NAT_MOB_NEX'
                                WHEN Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
                                WHEN Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
                                WHEN Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
                                WHEN Call_Destination_Code = 'TCRMG' THEN 'IN_ROAM_MT'
                                WHEN Call_Destination_Code = 'INT' THEN 'OUT_INT'
                                WHEN Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
                                ELSE Call_Destination_Code END
                            ) IN ('OUT_NAT_MOB_OCM', 'OUT_NAT_MOB_MTN', 'OUT_NAT_MOB_NEX', 'OUT_NAT_MOB_CAM', 'OUT_ROAM_MO')
                        THEN 1
                    ELSE 0 END
                ), 0) TRAFIC_SMS,
                NVL(SUM (
                    CASE 
                        WHEN 
                            (
                                CASE
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
                                END
                            ) IN ('VOI_VOX') THEN MAIN_RATED_AMOUNT 
                        ELSE  0  END 
                ), 0) REVENU_VOIX_PYG,
                NVL((SUM (MAIN_RATED_AMOUNT) - 
                    (SUM 
                    (CASE 
                        WHEN 
                        (
                            CASE
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
                            END
                        ) IN ('VOI_VOX') THEN MAIN_RATED_AMOUNT ELSE 0 END 
                    ) +
                    SUM 
                    (CASE 
                        WHEN 
                        (
                            CASE
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
                            END
                        ) NOT IN ('NVX_SMS','VOI_VOX') THEN MAIN_RATED_AMOUNT 
                    ELSE 0 END ))
                ), 0) REVENU_SMS_PYG 
            FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                AND MAIN_RATED_AMOUNT >= 0
                AND PROMO_RATED_AMOUNT >= 0
            GROUP BY 
                CHARGED_PARTY,
                SUBSTR(TRANSACTION_TIME, 1, 2)
            
            UNION ALL
            
            SELECT
                CHARGED_PARTY_MSISDN MSISDN,
                SUBSTR(SESSION_TIME, 1, 2) HOUR_PERIOD,
                SUM(0) TRAFIC_VOIX,
                NVL(SUM(NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0))/1024/1024, 0) TRAFIC_DATA, 
                SUM(0) TRAFIC_SMS,
                SUM(0) REVENU_VOIX_PYG,
                SUM(0) REVENU_SMS_PYG
            FROM MON.SPARK_FT_CRA_GPRS
            WHERE SESSION_DATE = '###SLICE_VALUE###' AND NVL(MAIN_COST, 0) >= 0
            GROUP BY 
                CHARGED_PARTY_MSISDN,
                SUBSTR(SESSION_TIME, 1, 2)
        ) T
        GROUP BY
            MSISDN,
            HOUR_PERIOD

        -- UNION ALL

        -- -- valorisation le volume restant des souscriptions arrivant à expiration à slice_value
        -- SELECT
        --     A.MSISDN MSISDN,
        --     A.HOUR_PERIOD HOUR_PERIOD,
        --     SUM(0) TRAFIC_VOIX, 
        --     SUM(0) TRAFIC_DATA, 
        --     SUM(0) TRAFIC_SMS, 
        --     SUM(0) REVENU_VOIX_PYG,
        --     SUM(
        --         CASE
        --         (
        --             WHEN DA_TYPE='TEL' THEN VALUE
        --             ELSE 0 END
        --         )
        --     ) REVENU_VOIX_SUBS,
        --     SUM(
        --         CASE
        --         (
        --             WHEN DA_TYPE='DATA' THEN VALUE
        --             ELSE 0 END
        --         )
        --     ) REVENU_DATA,
        --     SUM(0) REVENU_SMS_PYG,
        --     SUM(
        --         CASE
        --         (
        --             WHEN DA_TYPE='SMS' THEN VALUE
        --             ELSE 0 END
        --         )
        --     ) REVENU_SMS_SUBS
        -- FROM
        -- (
        --     SELECT 
        --         MSISDN,
        --         DA_ID,
        --         DA_TYPE,
        --         CASE
        --         (
        --             WHEN NVL(PPM, 0)=0 THEN BUNDLE_COST
        --             ELSE PPM*VOLUME_REMAINING END
        --         ) VALUE
        --     FROM
        --     (
        --         SELECT
        --             A.MSISDN MSISDN,
        --             A.DA_ID DA_ID,
        --             DA_TYPE,
        --             VOLUME_REMAINING,
        --             PPM, -- remplacer le label
        --             BUNDLE_COST -- remplacer le label
        --         FROM
        --         (
        --             SELECT 
        --             *
        --             FROM MON.SPARK_FT_MSISDN_DA_STATUS 
        --             WHERE 
        --                 EVENT_DATE='###SLICE_VALUE###' AND
        --                 TO_DATE(EXPIRY_DATE)='###SLICE_VALUE###'
        --         ) A 
        --         LEFT JOIN 
        --         ( 
        --             SELECT
        --             *
        --             FROM PPCM_TABLE -- remplacer le label
        --             WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###', 1)
        --         ) B 
        --         ON A.MSISDN=B.MSISDN AND A.DA_ID=B.DA_ID
        --     ) T
        --     GROUP BY
        --         MSISDN,
        --         HOUR_PERIOD
        -- ) T
        -- GROUP BY
        --     MSISDN,
        --     HOUR_PERIOD
    ) T
    GROUP BY
        MSISDN,
        HOUR_PERIOD
) A
LEFT JOIN
(
    SELECT
        MSISDN,
        HOUR_PERIOD,
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR
    WHERE EVENT_DATE = '###SLICE_VALUE###'
) B 
ON A.MSISDN = B.MSISDN AND A.HOUR_PERIOD = B.HOUR_PERIOD
LEFT JOIN
(
    SELECT
        MSISDN,
        TEK_RADIO 
    FROM
    (
        SELECT
            MSISDN
            , SUBSTR(IMEI, 1, 14) IMEI
        FROM MON.SPARK_FT_IMEI_ONLINE
        WHERE SDATE='###SLICE_VALUE###'
    ) C0
    LEFT JOIN DIM.DT_TAC_MOST_RECENT_TEK C1
    ON TRIM(SUBSTR(C0.IMEI, 1, 8)) = TRIM(C1.TAC)
) C
ON A.MSISDN = C.MSISDN

