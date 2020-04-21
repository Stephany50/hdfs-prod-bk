INSERT INTO AGG.SPARK_FT_A_REVENU_SITE_AMN
SELECT
    DISTINCT SITE_NAME,
    TYPE,
    CASE
        WHEN
            TYPE = 'DATA'
        THEN
            NULL
        ELSE
            DESTINATION
    END
    AS DESTINATION,
    NB_APPELS,
    VOLUME_TOTAL,
    REVENU_MAIN_KFCFA,
    REVENU_PROMO_KFCFA,
    CURRENT_TIMESTAMP() AS INSERT_DATE,
    'PREPAID' AS CONTRACT_TYPE,
    EVENT_DATE
FROM
(
    SELECT
        EVENT_DATE,
        SITE_NAME,
        CASE
        WHEN
            b.LAB = 'DATA_MAIN_AMOUNT'
        THEN
            'DATA'
        WHEN
            b.LAB = 'DATA_PROMO_AMOUNT'
        THEN
            'DATA'
        WHEN
            b.LAB = 'DATA_VOLUME_MO'
        THEN
            'DATA'
        WHEN
            b.LAB = 'TEL_CALL_COUNT'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'SMS_VOLUME'
        THEN
            'SMS'
        WHEN
            b.LAB = 'TEL_DURATION_MIN'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'VOICE_MAIN_AMOUNT'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'VOICE_PROMO_AMOUNT'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'SMS_MAIN_AMOUNT'
        THEN
            'SMS'
        WHEN
            b.LAB = 'SMS_PROMO_AMOUNT'
        THEN
            'SMS'
        END
        AS TYPE,
        DESTINATION,
        SUM(
            CASE
            WHEN
                b.LAB = 'TEL_CALL_COUNT'
            THEN
                TEL_CALL_COUNT
            ELSE
                0
            END
        ) AS NB_APPELS,
        SUM(
            CASE
            WHEN
                b.LAB = 'DATA_VOLUME_MO'
            THEN
                DATA_VOLUME_MO
            WHEN
                b.LAB = 'SMS_VOLUME'
            THEN
                SMS_VOLUME
            WHEN
                b.LAB = 'TEL_DURATION_MIN'
            THEN
                TEL_DURATION_MIN
            ELSE
                0
            END
        ) AS VOLUME_TOTAL,
        SUM(
            CASE
            WHEN
                b.LAB = 'DATA_MAIN_AMOUNT'
            THEN
                DATA_MAIN_AMOUNT / 1000
            WHEN
                b.LAB = 'VOICE_MAIN_AMOUNT'
            THEN
                VOICE_MAIN_AMOUNT / 1000
            WHEN
                b.LAB = 'SMS_MAIN_AMOUNT'
            THEN
                SMS_MAIN_AMOUNT / 1000
            ELSE
                0
            END
        ) AS REVENU_MAIN_KFCFA,
        SUM(
            CASE
            WHEN
                b.LAB = 'DATA_PROMO_AMOUNT'
            THEN
                DATA_PROMO_AMOUNT / 1000
            WHEN
                b.LAB = 'VOICE_PROMO_AMOUNT'
            THEN
                VOICE_PROMO_AMOUNT / 1000
            WHEN
                b.LAB = 'SMS_PROMO_AMOUNT'
            THEN
                SMS_PROMO_AMOUNT / 1000
            ELSE
                0
            END
        ) AS REVENU_PROMO_KFCFA
    FROM
    (
        SELECT
            COALESCE(a.EVENT_DATE, b.EVENT_DATE) AS EVENT_DATE,
            COALESCE(a.SITE_NAME, b.SITE_NAME) AS SITE_NAME,
            a.DESTINATION,
            b.MAIN_COST AS DATA_MAIN_AMOUNT,
            b.PROMO_COST AS DATA_PROMO_AMOUNT,
            b.BYTES_RECEIVED / (1204*1024) AS DATA_VOLUME_Mo,
            a.RATED_TEL_TOTAL_COUNT AS TEL_CALL_COUNT,
            a.RATED_SMS_TOTAL_COUNT AS SMS_VOLUME,
            a.RATED_DURATION / 60 AS TEL_DURATION_MIN,
            a.VOICE_MAIN_RATED_AMOUNT AS VOICE_MAIN_AMOUNT,
            a.VOICE_PROMO_RATED_AMOUNT AS VOICE_PROMO_AMOUNT,
            a.SMS_MAIN_RATED_AMOUNT AS SMS_MAIN_AMOUNT,
            a.SMS_PROMO_RATED_AMOUNT AS SMS_PROMO_AMOUNT
        FROM
        (
            SELECT
                TRANSACTION_DATE AS EVENT_DATE,
                SITE_NAME,
                CASE
                WHEN
                    DESTINATION = 'Orange'
                THEN
                    'OnNet'
                WHEN
                    DESTINATION IN
                    (
                        'MTN', 'Camtel', 'NEXTTEL'
                    )
                THEN
                    'OffNet'
                WHEN
                    DESTINATION = 'International'
                THEN
                    'International'
                END
                AS DESTINATION,
                SUM(RATED_TEL_TOTAL_COUNT) AS RATED_TEL_TOTAL_COUNT,
                SUM(RATED_SMS_TOTAL_COUNT) AS RATED_SMS_TOTAL_COUNT,
                SUM(RATED_DURATION) AS RATED_DURATION,
                SUM(VOICE_MAIN_RATED_AMOUNT) AS VOICE_MAIN_RATED_AMOUNT,
                SUM(VOICE_PROMO_RATED_AMOUNT) AS VOICE_PROMO_RATED_AMOUNT,
                SUM(SMS_MAIN_RATED_AMOUNT) AS SMS_MAIN_RATED_AMOUNT,
                SUM(SMS_PROMO_RATED_AMOUNT) AS SMS_PROMO_RATED_AMOUNT
            FROM
            (
                select
                    transaction_date,
                    vdci.SITE_NAME AS site_name,
                    CASE
                        WHEN
                            b.dest_short = 'Orange'
                        THEN
                            'Orange'
                        WHEN
                            b.dest_short = 'MTN'
                        THEN
                            'MTN'
                        WHEN
                            b.dest_short = 'International'
                        THEN
                            'International'
                        WHEN
                            b.dest_short = 'SVA'
                        THEN
                            'SVA'
                        WHEN
                            b.dest_short LIKE '%MVNO%'
                        THEN
                            'Orange'
                        WHEN
                            b.dest_short = 'CTPhone'
                        THEN
                            'Camtel'
                        WHEN
                            b.dest_short LIKE '%Roam%'
                        THEN
                            'roaming'
                        WHEN
                            b.dest_short = 'NEXTTEL'
                        THEN
                            'NEXTTEL'
                        ELSE
                            'AUTRES'
                    END
                    destination ,
                    sum(
                    CASE
                        WHEN
                            service_code = 'VOI_VOX'
                        THEN
                            rated_total_count
                        ELSE
                            0
                    end
                    ) RATED_TEL_TOTAL_COUNT,
                    sum(
                    CASE
                        WHEN
                            service_code = 'NVX_SMS'
                        THEN
                            rated_total_count
                        ELSE
                            0
                    end
                    ) RATED_SMS_TOTAL_COUNT,
                    sum(
                    CASE
                        WHEN
                            service_code = 'VOI_VOX'
                        THEN
                            rated_duration
                        ELSE
                            0
                    END
                    ) RATED_DURATION,
                    sum(
                    CASE
                        WHEN
                            service_code = 'VOI_VOX'
                        THEN
                            promo_rated_amount
                        ELSE
                            0
                    end
                    ) VOICE_PROMO_RATED_AMOUNT,
                    sum(
                    CASE
                        WHEN
                            service_code = 'NVX_SMS'
                        THEN
                            main_rated_amount
                        ELSE
                            0
                    end
                    ) SMS_MAIN_RATED_AMOUNT,
                    sum(
                    CASE
                        WHEN
                            service_code = 'VOI_VOX'
                        THEN
                            main_rated_amount
                        ELSE
                            0
                    end
                    ) VOICE_MAIN_RATED_AMOUNT,
                    sum(
                    CASE
                        WHEN
                            service_code = 'NVX_SMS'
                        THEN
                            promo_rated_amount
                        ELSE
                            0
                    end
                    ) SMS_PROMO_RATED_AMOUNT
                from
                (
                    SELECT *
                    FROM MON.SPARK_FT_GSM_LOCATION_REVENUE_DAILY
                    where TRANSACTION_DATE = '###SLICE_VALUE###'
                ) a
                inner join
                (
                    select
                    LAC,
                    (
                        Case
                            when
                                length(CI) = 3
                            then
                                concat('00', CI)
                            when
                                length(CI) = 4
                            then
                                concat('0', CI)
                            else
                                CI
                        end
                    )
                    CI , SITE_NAME
                    from
                    dim.dt_ci_lac_site_amn
                ) vdci
                on LPAD(CONV(upper(NSL_CI), 16, 10), 5, 0) = vdci.CI
                right join
                (
                    select
                    dest_id,
                    dest_short
                    from
                    dim.dt_destinations
                ) b
                on b.dest_id = destination
                group BY
                    transaction_date,
                    vdci.SITE_NAME,
                    CASE
                        WHEN
                            b.dest_short = 'Orange'
                        THEN
                            'Orange'
                        WHEN
                            b.dest_short = 'MTN'
                        THEN
                            'MTN'
                        WHEN
                            b.dest_short = 'International'
                        THEN
                            'International'
                        WHEN
                            b.dest_short = 'SVA'
                        THEN
                            'SVA'
                        WHEN
                            b.dest_short LIKE '%MVNO%'
                        THEN
                            'Orange'
                        WHEN
                            b.dest_short = 'CTPhone'
                        THEN
                            'Camtel'
                        WHEN
                            b.dest_short LIKE '%Roam%'
                        THEN
                            'roaming'
                        WHEN
                            b.dest_short = 'NEXTTEL'
                        THEN
                            'NEXTTEL'
                        ELSE
                            'AUTRES'
                    END
            ) b
            WHERE
                DESTINATION IN
                (
                'Orange', 'International', 'MTN', 'Camtel', 'NEXTTEL'
                )
            GROUP BY
                TRANSACTION_DATE, SITE_NAME,
                CASE
                WHEN
                    DESTINATION = 'Orange'
                THEN
                    'OnNet'
                WHEN
                    DESTINATION IN
                    (
                        'MTN', 'Camtel', 'NEXTTEL'
                    )
                THEN
                    'OffNet'
                WHEN
                    DESTINATION = 'International'
                THEN
                    'International'
                END
        ) a
        FULL JOIN
        (
            SELECT
                EVENT_DATE,
                SITE_NAME,
                SUM(MAIN_COST) AS MAIN_COST,
                SUM(PROMO_COST) AS PROMO_COST,
                SUM(BYTES_RECEIVED) AS BYTES_RECEIVED
            FROM
            (
                SELECT
                    a.SESSION_DATE AS EVENT_DATE,
                    vdci.SITE_NAME AS SITE_NAME,
                    SUM (main_cost) main_cost,
                    SUM (promo_cost) promo_cost,
                    SUM (bytes_received) bytes_received
                FROM AGG.SPARK_FT_A_GPRS_LOCATION a
                inner join
                (
                    select
                        LAC,
                        (
                            Case
                                when
                                length(CI) = 3
                                then
                                concat('00', CI)
                                when
                                length(CI) = 4
                                then
                                concat('0', CI)
                                else
                                CI
                            end
                        )
                        CI, SITE_NAME
                    from dim.dt_ci_lac_site_amn
                ) vdci
                on LOCATION_CI = vdci.CI
                WHERE
                    a.SESSION_DATE = '###SLICE_VALUE###'
                GROUP BY
                    a.SESSION_DATE, vdci.SITE_NAME
            ) c
            GROUP BY EVENT_DATE, SITE_NAME
        ) b
        ON (a.SITE_NAME = b.SITE_NAME)
    ) a
    CROSS JOIN
    (
        SELECT
            'DATA_MAIN_AMOUNT' AS LAB
        UNION
        SELECT
            'DATA_PROMO_AMOUNT' AS LAB
        UNION
        SELECT
            'DATA_VOLUME_MO' AS LAB
        UNION
        SELECT
            'TEL_CALL_COUNT' AS LAB
        UNION
        SELECT
            'SMS_VOLUME' AS LAB
        UNION
        SELECT
            'TEL_DURATION_MIN' AS LAB
        UNION
        SELECT
            'VOICE_MAIN_AMOUNT' AS LAB
        UNION
        SELECT
            'VOICE_PROMO_AMOUNT' AS LAB
        UNION
        SELECT
            'SMS_MAIN_AMOUNT' AS LAB
        UNION
        SELECT
            'SMS_PROMO_AMOUNT' AS LAB
    ) b
    GROUP BY
        EVENT_DATE,
        SITE_NAME,
        DESTINATION,
        CASE
        WHEN
            b.LAB = 'DATA_MAIN_AMOUNT'
        THEN
            'DATA'
        WHEN
            b.LAB = 'DATA_PROMO_AMOUNT'
        THEN
            'DATA'
        WHEN
            b.LAB = 'DATA_VOLUME_MO'
        THEN
            'DATA'
        WHEN
            b.LAB = 'TEL_CALL_COUNT'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'SMS_VOLUME'
        THEN
            'SMS'
        WHEN
            b.LAB = 'TEL_DURATION_MIN'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'VOICE_MAIN_AMOUNT'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'VOICE_PROMO_AMOUNT'
        THEN
            'VOIX'
        WHEN
            b.LAB = 'SMS_MAIN_AMOUNT'
        THEN
            'SMS'
        WHEN
            b.LAB = 'SMS_PROMO_AMOUNT'
        THEN
            'SMS'
        END
) a