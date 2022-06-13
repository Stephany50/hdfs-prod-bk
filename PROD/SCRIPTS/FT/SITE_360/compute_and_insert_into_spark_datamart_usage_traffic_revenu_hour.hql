INSERT INTO MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
SELECT
    A.MSISDN MSISDN,
    A.HOUR_PERIOD EVENT_HOUR,
    NVL(C.DEVICE_TYPE, D.DEVICE_TYPE) DEVICE_TYPE,
    NVL 
    (
        CASE
            WHEN device_rank = 7 THEN '5G'
            WHEN device_rank = 6 THEN '4G'
            WHEN device_rank = 5 THEN '3G'
            WHEN device_rank = 4 THEN '2.75G'
            WHEN device_rank = 3 THEN '2.5G'
            WHEN device_rank = 2 THEN '2G'
        ELSE NULL END, D.TECHNO_DEVICE
    ) TECHNO_DEVICE,
    NVL(B.TRAFIC_VOIX, 0), 
    NVL(B.TRAFIC_DATA, 0), 
    NVL(B.TRAFIC_SMS, 0), 
    NVL(B.REVENU_VOIX_PYG, 0),
    NVL(B.REVENU_VOIX_SUBS, 0), 
    NVL(B.REVENU_DATA, 0), 
    NVL(B.REVENU_SMS_PYG, 0),
    NVL(B.REVENU_SMS_SUBS, 0), 
    A.SITE_NAME,
    A.TOWN,
    A.REGION,
    A.COMMERCIAL_REGION,
    CURRENT_TIMESTAMP INSERT_DATE,
    A.EST_PARC_GROUPE EST_PARC_GROUPE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        MSISDN,
        HOUR_PERIOD,
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION,
        EST_PARC_GROUPE
    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR
    WHERE EVENT_DATE = '###SLICE_VALUE###' 
) A 
LEFT JOIN
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
) B
ON A.MSISDN = B.MSISDN AND A.HOUR_PERIOD = B.HOUR_PERIOD
LEFT JOIN
(
    select
        msisdn,
        device_type,
        device_rank
    from
    (
        select
            msisdn,
            first_value(device_type) over(partition by msisdn order by device_rank desc) device_type,
            ROW_NUMBER() over(partition by msisdn order by device_rank desc) line_number,
            max(device_rank) over(partition by msisdn order by device_rank desc) device_rank
        from
        (
            SELECT
                MSISDN,
                (
                    CASE
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) = '5G' THEN 7
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('LTE CA', 'LTE') THEN 6
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('3G', 'HSDPA', '3G EDGE', 'HSPA', 'HSPA+', 'HSUPA', 'UMTS') THEN 5
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('EDGE') THEN 4
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('GPRS') THEN 3
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('2G', 'GSM') THEN 2
                        WHEN TRIM(UPPER(C.LTE)) = 'YES' THEN 6
                        WHEN TRIM(UPPER(C.HSDPA_FLAG)) = 'T' THEN 5
                        WHEN TRIM(UPPER(C.HSUPA_FLAG)) = 'T' THEN 5
                        WHEN TRIM(UPPER(C.UMTS_FLAG)) = 'T' THEN 5
                        WHEN TRIM(UPPER(C.EDGE_FLAG)) = 'T' THEN 4
                        WHEN TRIM(UPPER(C.GPRS_FLAG)) = 'T' THEN 3
                        WHEN TRIM(UPPER(C.GSM_BAND_FLAG)) = 'T' THEN 2
                        WHEN TRIM(UPPER(A.TECHNOLOGIE)) = '4G' THEN 6
                        WHEN TRIM(UPPER(A.TECHNOLOGIE)) = '3G' THEN 5
                        WHEN TRIM(UPPER(A.TECHNOLOGIE)) = '2.75G' THEN 4
                        WHEN TRIM(UPPER(A.TECHNOLOGIE)) = '2.5G' THEN 3
                        WHEN TRIM(UPPER(A.TECHNOLOGIE)) = '2G' THEN 2
                        ELSE 1
                    END
                ) device_rank,
                device_type
            FROM
            (
                SELECT
                    MSISDN
                    , SUBSTR(IMEI, 1, 14) IMEI
                FROM MON.SPARK_FT_IMEI_ONLINE
                WHERE SDATE='###SLICE_VALUE###'
            ) C0
            LEFT JOIN DIM.DT_NEW_HANDSET_REF C
            ON lpad(TRIM(SUBSTR(C0.IMEI, 1, 8)), 8, 0) = TRIM(C.TAC)
            left join 
            (
                select * 
                from DIM.DT_HANDSET_REF L1
            ) A on lpad(TRIM(SUBSTR(C0.IMEI, 1, 8)), 8, 0) = lpad(TRIM(SUBSTR(A.TAC_CODE, 1, 8)), 8, 0)
        ) T
    ) T
    where line_number = 1
) C
ON A.MSISDN = C.MSISDN
LEFT JOIN 
(
    SELECT 
        *
    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
    WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1) AND EVENT_HOUR='23'
) D
ON A.MSISDN = D.MSISDN 