INSERT INTO AGG.SPARK_FT_A_REVENU_SITE_AMN
SELECT
    SITE_NAME
    , TYPE
    , CASE
        WHEN TYPE = 'DATA' THEN NULL
        ELSE DESTINATION
    END AS DESTINATION
    , NB_APPELS
    , VOLUME_TOTAL
    , REVENU_MAIN_KFCFA
    , REVENU_PROMO_KFCFA
    , CURRENT_TIMESTAMP() AS INSERT_DATE
    , 'POSTPAID' AS CONTRACT_TYPE
    ,DISTINCT EVENT_DATE
FROM
(
    SELECT
        EVENT_DATE
        , SITE_NAME
        , CASE
            WHEN b.LAB = 'DATA_MAIN_AMOUNT' THEN 'DATA'
            WHEN b.LAB = 'DATA_PROMO_AMOUNT' THEN 'DATA'
            WHEN b.LAB = 'DATA_VOLUME_MO' THEN 'DATA'
            WHEN b.LAB = 'TEL_CALL_COUNT' THEN 'VOIX'
            WHEN b.LAB = 'SMS_VOLUME' THEN 'SMS'
            WHEN b.LAB = 'TEL_DURATION_MIN' THEN 'VOIX'
            WHEN b.LAB = 'VOICE_MAIN_AMOUNT' THEN 'VOIX'
            WHEN b.LAB = 'VOICE_PROMO_AMOUNT' THEN 'VOIX'
            WHEN b.LAB = 'SMS_MAIN_AMOUNT' THEN 'SMS'
            WHEN b.LAB = 'SMS_PROMO_AMOUNT' THEN 'SMS'
        END AS TYPE
        , DESTINATION
        , SUM(
            CASE
                WHEN b.LAB = 'TEL_CALL_COUNT' THEN TEL_CALL_COUNT
                ELSE 0
            END
        ) AS NB_APPELS
        , SUM(
            CASE
                WHEN b.LAB = 'DATA_VOLUME_MO' THEN DATA_VOLUME_MO
                WHEN b.LAB = 'SMS_VOLUME' THEN SMS_VOLUME
                WHEN b.LAB = 'TEL_DURATION_MIN' THEN TEL_DURATION_MIN
                ELSE 0
            END
        ) AS VOLUME_TOTAL
        , SUM(
            CASE
                WHEN b.LAB = 'DATA_MAIN_AMOUNT' THEN DATA_MAIN_AMOUNT / 1000
                WHEN b.LAB = 'VOICE_MAIN_AMOUNT' THEN VOICE_MAIN_AMOUNT / 1000
                WHEN b.LAB = 'SMS_MAIN_AMOUNT' THEN SMS_MAIN_AMOUNT / 1000
                ELSE 0
            END
        ) AS REVENU_MAIN_KFCFA
        , SUM(
            CASE
                WHEN b.LAB = 'DATA_PROMO_AMOUNT' THEN DATA_PROMO_AMOUNT / 1000
                WHEN b.LAB = 'VOICE_PROMO_AMOUNT' THEN VOICE_PROMO_AMOUNT / 1000
                WHEN b.LAB = 'SMS_PROMO_AMOUNT' THEN SMS_PROMO_AMOUNT / 1000
                ELSE 0
            END
        ) AS REVENU_PROMO_KFCFA
    FROM
    (
        SELECT
            COALESCE(a.EVENT_DATE, b.EVENT_DATE) AS EVENT_DATE
            , COALESCE(a.SITE_NAME, b.SITE_NAME) AS SITE_NAME
            , a.DESTINATION
            , b.MAIN_COST AS DATA_MAIN_AMOUNT
            , b.PROMO_COST AS DATA_PROMO_AMOUNT
            , b.BYTES_RECEIVED /1204/1024 AS DATA_VOLUME_MO
            , a.RATED_TEL_TOTAL_COUNT AS TEL_CALL_COUNT
            , a.RATED_SMS_TOTAL_COUNT AS SMS_VOLUME
            , a.RATED_DURATION / 60 AS TEL_DURATION_MIN
            , a.VOICE_MAIN_RATED_AMOUNT AS VOICE_MAIN_AMOUNT
            , a.VOICE_PROMO_RATED_AMOUNT AS VOICE_PROMO_AMOUNT
            , a.SMS_MAIN_RATED_AMOUNT AS SMS_MAIN_AMOUNT
            , a.SMS_PROMO_RATED_AMOUNT AS SMS_PROMO_AMOUNT
        FROM
        (
            SELECT
                TRANSACTION_DATE AS EVENT_DATE
                , SITE_NAME
                , CASE
                    WHEN DESTINATION = 'Orange' THEN 'OnNet'
                    WHEN DESTINATION IN ('MTN', 'Camtel', 'NEXTTEL')  THEN 'OffNet'
                    WHEN DESTINATION = 'International' THEN 'International'
                END AS DESTINATION
                , SUM(RATED_TEL_TOTAL_COUNT) AS RATED_TEL_TOTAL_COUNT
                , SUM(RATED_SMS_TOTAL_COUNT) AS RATED_SMS_TOTAL_COUNT
                , SUM(RATED_DURATION) AS RATED_DURATION
                , SUM(VOICE_MAIN_RATED_AMOUNT) AS VOICE_MAIN_RATED_AMOUNT
                , SUM(VOICE_PROMO_RATED_AMOUNT) AS VOICE_PROMO_RATED_AMOUNT
                , SUM(SMS_MAIN_RATED_AMOUNT) AS SMS_MAIN_RATED_AMOUNT
                , SUM(SMS_PROMO_RATED_AMOUNT) AS SMS_PROMO_RATED_AMOUNT
            FROM
            (
                select
                    transaction_date
                    , vdci.SITE_NAME AS site_name
                    , CASE
                        WHEN  b.dest_short='Orange' THEN 'Orange'
                        WHEN  b.dest_short='MTN'   THEN 'MTN'
                        WHEN  b.dest_short='International' THEN 'International'
                        WHEN  b.dest_short='SVA' THEN 'SVA'
                        WHEN  b.dest_short LIKE '%MVNO%' THEN 'Orange'
                        WHEN  b.dest_short = 'CTPhone' THEN 'Camtel'
                        WHEN  b.dest_short LIKE '%Roam%' THEN 'roaming'
                        WHEN  b.dest_short = 'NEXTTEL' THEN 'NEXTTEL'
                        ELSE 'AUTRES'
                    END destination
                    , sum(CASE WHEN service_code='VOI_VOX' THEN rated_total_count ELSE 0 end) RATED_TEL_TOTAL_COUNT
                    , sum(CASE WHEN service_code='NVX_SMS' THEN rated_total_count ELSE 0 end) RATED_SMS_TOTAL_COUNT
                    , sum(CASE WHEN service_code='VOI_VOX' THEN rated_duration ELSE 0 END ) RATED_DURATION
                    , sum(CASE WHEN service_code='VOI_VOX' THEN promo_rated_amount ELSE 0 end) VOICE_PROMO_RATED_AMOUNT
                    , sum(CASE WHEN service_code='NVX_SMS' THEN main_rated_amount ELSE 0 end) SMS_MAIN_RATED_AMOUNT
                    , sum(CASE WHEN service_code='VOI_VOX' THEN main_rated_amount ELSE 0 end) VOICE_MAIN_RATED_AMOUNT
                    , sum(CASE WHEN service_code='NVX_SMS' THEN promo_rated_amount ELSE 0 end) SMS_PROMO_RATED_AMOUNT
                from (
                    SELECT
                        TRANSACTION_DATE
                        ,(
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
                        ) AS SERVICE_CODE
                        ,(
                            CASE
                                WHEN Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
                                WHEN Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                                WHEN Call_Destination_Code IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
                                WHEN Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
                                WHEN Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                                WHEN Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
                                WHEN Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
                                WHEN Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
                                WHEN Call_Destination_Code = 'INT' THEN 'OUT_INT'
                                WHEN Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
                                ELSE Call_Destination_Code
                            END
                        ) AS DESTINATION
                        , (
                            CASE
                                WHEN (LOCATION_MCC<>'624') or (LOCATION_MNC<>'02') THEN ''
                                ELSE LOCATION_CI END
                        ) AS NSL_CI
                        , CASE WHEN Main_Rated_Amount + Promo_Rated_Amount > 0 THEN 1 ELSE 0 END AS RATED_TOTAL_COUNT
                        , CASE WHEN Main_Rated_Amount + Promo_Rated_Amount > 0 THEN CALL_PROCESS_TOTAL_DURATION ELSE 0 END AS RATED_DURATION
                        , PROMO_RATED_AMOUNT
                        , MAIN_RATED_AMOUNT
                    FROM mon.SPARK_FT_BILLED_TRANSACTION_POSTPAID
                    WHERE
                        TRANSACTION_DATE ='###SLICE_VALUE###'
                        AND Main_Rated_Amount >= 0
                        AND Promo_Rated_Amount >= 0
                ) a
                inner join dim.dt_ci_lac_site_amn vdci
                on LPAD(CONV(upper(NSL_CI), 16, 10),5,0) = vdci.ci
                right join (select dest_id, dest_short from dim.dt_destinations) b
                on b.dest_id=destination
                group BY transaction_date
                    , vdci.SITE_NAME
                    , CASE
                        WHEN  b.dest_short='Orange' THEN 'Orange'
                        WHEN  b.dest_short='MTN'   THEN 'MTN'
                        WHEN  b.dest_short='International' THEN 'International'
                        WHEN  b.dest_short='SVA' THEN 'SVA'
                        WHEN  b.dest_short LIKE '%MVNO%' THEN 'Orange'
                        WHEN  b.dest_short = 'CTPhone' THEN 'Camtel'
                        WHEN  b.dest_short LIKE '%Roam%' THEN 'roaming'
                        WHEN  b.dest_short = 'NEXTTEL' THEN 'NEXTTEL'
                        ELSE 'AUTRES'
                    END
            ) a
            WHERE DESTINATION IN ('Orange', 'International', 'MTN', 'Camtel', 'NEXTTEL')
            GROUP BY TRANSACTION_DATE,
                SITE_NAME,
                CASE
                    WHEN DESTINATION = 'Orange' THEN 'OnNet'
                    WHEN DESTINATION IN ('MTN', 'Camtel', 'NEXTTEL') THEN 'OffNet'
                    WHEN DESTINATION = 'International' THEN 'International'
                END
        ) a
        FULL JOIN
        (
            SELECT
                EVENT_DATE
                , SITE_NAME
                , SUM(MAIN_COST) AS MAIN_COST
                , SUM(PROMO_COST) AS PROMO_COST
                , SUM(BYTES_RECEIVED) AS BYTES_RECEIVED
            FROM
            (
                SELECT
                    a.SESSION_DATE AS EVENT_DATE
                    , vdci.SITE_NAME AS SITE_NAME
                    , SUM (MAIN_COST) MAIN_COST
                    , SUM (PROMO_COST) PROMO_COST
                    , SUM (BYTES_RECEIVED) BYTES_RECEIVED
                FROM
                (
                    SELECT
                        SESSION_DATE
                        , LOCATION_CI
                        , MAIN_COST
                        , PROMO_COST
                        , BYTES_RECEIVED
                    FROM MON.SPARK_FT_CRA_GPRS_POST
                    WHERE SESSION_DATE = '###SLICE_VALUE###'
                ) a
                inner join
                (
                    select
                        (
                            case when length(ci)=4 then '0'||ci
                            when length(ci) =3 then '00'||ci
                            else ci end
                        ) ci
                        , site_name
                    from
                    (
                        select
                            ci
                            , site_name
                        from dim.dt_ci_lac_site_amn
                    ) b
                ) vdci
                on LOCATION_CI = vdci.CI
                GROUP BY a.SESSION_DATE, vdci.SITE_NAME
            ) a
            GROUP BY EVENT_DATE, SITE_NAME
        ) b
        ON a.SITE_NAME = b.SITE_NAME
    ) a
    CROSS JOIN
    (
        SELECT 'DATA_MAIN_AMOUNT' AS LAB UNION
        SELECT 'DATA_PROMO_AMOUNT' AS LAB UNION
        SELECT 'DATA_VOLUME_MO' AS LAB UNION
        SELECT 'TEL_CALL_COUNT' AS LAB UNION
        SELECT 'SMS_VOLUME' AS LAB UNION
        SELECT 'TEL_DURATION_MIN' AS LAB UNION
        SELECT 'VOICE_MAIN_AMOUNT' AS LAB UNION
        SELECT 'VOICE_PROMO_AMOUNT' AS LAB UNION
        SELECT 'SMS_MAIN_AMOUNT' AS LAB UNION
        SELECT 'SMS_PROMO_AMOUNT' AS LAB
    ) b
    GROUP BY EVENT_DATE,
        SITE_NAME,
        DESTINATION,
        CASE
            WHEN b.LAB = 'DATA_MAIN_AMOUNT' THEN 'DATA'
            WHEN b.LAB = 'DATA_PROMO_AMOUNT' THEN 'DATA'
            WHEN b.LAB = 'DATA_VOLUME_MO' THEN 'DATA'
            WHEN b.LAB = 'TEL_CALL_COUNT' THEN 'VOIX'
            WHEN b.LAB = 'SMS_VOLUME' THEN 'SMS'
            WHEN b.LAB = 'TEL_DURATION_MIN' THEN 'VOIX'
            WHEN b.LAB = 'VOICE_MAIN_AMOUNT' THEN 'VOIX'
            WHEN b.LAB = 'VOICE_PROMO_AMOUNT' THEN 'VOIX'
            WHEN b.LAB = 'SMS_MAIN_AMOUNT' THEN 'SMS'
            WHEN b.LAB = 'SMS_PROMO_AMOUNT' THEN 'SMS'
        END
) a