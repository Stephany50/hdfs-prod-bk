INSERT INTO MON.FT_MSISDN_PREMIUM_MONTH PARTITION(EVENT_MONTH)
SELECT
    MSISDN,
    REV_MOYEN,
    PREMIUM,
    CONSO_MOY_DATA,
    RECHARGE_MOY,
    CASE
        WHEN PREMIUM = 1 AND CONSO_MOY_DATA >= 50 AND RECHARGE_MOY <= 5 THEN 1
        WHEN PREMIUM = 1 AND (CONSO_MOY_DATA < 50 OR RECHARGE_MOY > 5) THEN 0
        ELSE 0
    END AS PRENIUM_PLUS,
    CURRENT_TIMESTAMP() AS INSERT_DATE,
    "###SLICE_VALUE###" AS EVENT_MONTH
FROM
(
    SELECT
        MSISDN,
        REV_MOYEN,
        CASE
            WHEN REV_MOYEN >= 5000 THEN 1
            ELSE 0
        END AS PREMIUM,
        CONSO_MOY_DATA,
        RECHARGE_MOY
    FROM
    (
        SELECT
            MSISDN,
            (MAIN_RATED_TEL_AMOUNT + MAIN_RATED_SMS_AMOUNT + DATA_MAIN_RATED_AMOUNT + TOTAL_SUBS_REVENUE + DATA_GOS_MAIN_RATED_AMOUNT) / 3  as REV_MOYEN,
            (DATA_MAIN_RATED_AMOUNT + SUBS_DATA_AMOUNT) / 3 as CONSO_MOY_DATA,
            ROUND((C2S_REFILL_COUNT + P2P_REFILL_COUNT + SCRATCH_REFILL_COUNT)/3)  as RECHARGE_MOY
        FROM
        (
            -- jointure complete des donnees des trois mois avec sommation des indicateurs par numéros
            SELECT
                COALESCE(a1.MSISDN, a2.MSISDN) AS MSISDN,
                COALESCE(a1.SUBS_DATA_AMOUNT, 0) + COALESCE(a2.SUBS_DATA_AMOUNT, 0) AS SUBS_DATA_AMOUNT,
                COALESCE(a1.MAIN_RATED_TEL_AMOUNT, 0) + COALESCE(a2.MAIN_RATED_TEL_AMOUNT, 0) AS MAIN_RATED_TEL_AMOUNT,
                COALESCE(a1.MAIN_RATED_SMS_AMOUNT , 0) + COALESCE(a2.MAIN_RATED_SMS_AMOUNT , 0) AS MAIN_RATED_SMS_AMOUNT ,
                COALESCE(a1.DATA_MAIN_RATED_AMOUNT, 0) + COALESCE(a2.DATA_MAIN_RATED_AMOUNT, 0) AS DATA_MAIN_RATED_AMOUNT,
                COALESCE(a1.TOTAL_SUBS_REVENUE, 0) + COALESCE(a2.TOTAL_SUBS_REVENUE, 0) AS TOTAL_SUBS_REVENUE,
                COALESCE(a1.DATA_GOS_MAIN_RATED_AMOUNT, 0) + COALESCE(a2.DATA_GOS_MAIN_RATED_AMOUNT, 0) AS DATA_GOS_MAIN_RATED_AMOUNT,
                COALESCE(a1.C2S_REFILL_COUNT, 0) + COALESCE(a2.C2S_REFILL_COUNT, 0) AS C2S_REFILL_COUNT,
                COALESCE(a1.P2P_REFILL_COUNT, 0) + COALESCE(a2.P2P_REFILL_COUNT, 0) AS P2P_REFILL_COUNT,
                COALESCE(a1.SCRATCH_REFILL_COUNT, 0) + COALESCE(a2.SCRATCH_REFILL_COUNT, 0) AS SCRATCH_REFILL_COUNT
            FROM
            (
                SELECT
                    COALESCE(a1.MSISDN, a2.MSISDN) AS MSISDN,
                    COALESCE(a1.SUBS_DATA_AMOUNT, 0) + COALESCE(a2.SUBS_DATA_AMOUNT, 0) AS SUBS_DATA_AMOUNT,
                    COALESCE(a1.MAIN_RATED_TEL_AMOUNT, 0) + COALESCE(a2.MAIN_RATED_TEL_AMOUNT, 0) AS MAIN_RATED_TEL_AMOUNT,
                    COALESCE(a1.MAIN_RATED_SMS_AMOUNT , 0) + COALESCE(a2.MAIN_RATED_SMS_AMOUNT , 0) AS MAIN_RATED_SMS_AMOUNT ,
                    COALESCE(a1.DATA_MAIN_RATED_AMOUNT, 0) + COALESCE(a2.DATA_MAIN_RATED_AMOUNT, 0) AS DATA_MAIN_RATED_AMOUNT,
                    COALESCE(a1.TOTAL_SUBS_REVENUE, 0) + COALESCE(a2.TOTAL_SUBS_REVENUE, 0) AS TOTAL_SUBS_REVENUE,
                    COALESCE(a1.DATA_GOS_MAIN_RATED_AMOUNT, 0) + COALESCE(a2.DATA_GOS_MAIN_RATED_AMOUNT, 0) AS DATA_GOS_MAIN_RATED_AMOUNT,
                    COALESCE(a1.C2S_REFILL_COUNT, 0) + COALESCE(a2.C2S_REFILL_COUNT, 0) AS C2S_REFILL_COUNT,
                    COALESCE(a1.P2P_REFILL_COUNT, 0) + COALESCE(a2.P2P_REFILL_COUNT, 0) AS P2P_REFILL_COUNT,
                    COALESCE(a1.SCRATCH_REFILL_COUNT, 0) + COALESCE(a2.SCRATCH_REFILL_COUNT, 0) AS SCRATCH_REFILL_COUNT
                FROM
                (
                    SELECT
                        MSISDN,
                        SUBS_DATA_AMOUNT,
                        MAIN_RATED_TEL_AMOUNT,
                        MAIN_RATED_SMS_AMOUNT,
                        DATA_MAIN_RATED_AMOUNT,
                        TOTAL_SUBS_REVENUE,
                        DATA_GOS_MAIN_RATED_AMOUNT,
                        C2S_REFILL_COUNT,
                        P2P_REFILL_COUNT,
                        SCRATCH_REFILL_COUNT
                    FROM
                        MON.FT_MARKETING_DATAMART_MONTH
                    WHERE
                        EVENT_MONTH = date_format(add_months(from_unixtime(unix_timestamp('###SLICE_VALUE###' , 'yyyyMM')), -1), 'yyyyMM')
                        AND CONTRACT_TYPE IN ('PURE PREPAID', 'HYBRID')
                ) a1
                FULL JOIN
                (
                    SELECT
                        MSISDN,
                        SUBS_DATA_AMOUNT,
                        MAIN_RATED_TEL_AMOUNT,
                        MAIN_RATED_SMS_AMOUNT,
                        DATA_MAIN_RATED_AMOUNT,
                        TOTAL_SUBS_REVENUE,
                        DATA_GOS_MAIN_RATED_AMOUNT,
                        C2S_REFILL_COUNT,
                        P2P_REFILL_COUNT,
                        SCRATCH_REFILL_COUNT
                    FROM
                        MON.FT_MARKETING_DATAMART_MONTH
                    WHERE
                        EVENT_MONTH = date_format(add_months(from_unixtime(unix_timestamp('###SLICE_VALUE###' , 'yyyyMM')), -2), 'yyyyMM')
                        AND CONTRACT_TYPE IN ('PURE PREPAID', 'HYBRID')
                ) a2
                    ON a1.MSISDN = a2.MSISDN
            ) a1
            FULL JOIN
            (
                SELECT
                    MSISDN,
                    SUBS_DATA_AMOUNT,
                    MAIN_RATED_TEL_AMOUNT,
                    MAIN_RATED_SMS_AMOUNT,
                    DATA_MAIN_RATED_AMOUNT,
                    TOTAL_SUBS_REVENUE,
                    DATA_GOS_MAIN_RATED_AMOUNT,
                    C2S_REFILL_COUNT,
                    P2P_REFILL_COUNT,
                    SCRATCH_REFILL_COUNT
                FROM
                    MON.FT_MARKETING_DATAMART_MONTH
                WHERE
                    EVENT_MONTH = date_format(add_months(from_unixtime(unix_timestamp('###SLICE_VALUE###' , 'yyyyMM')), -3), 'yyyyMM')
                    AND CONTRACT_TYPE IN ('PURE PREPAID', 'HYBRID')
            ) a2
                ON a1.MSISDN = a2.MSISDN
        ) C
    ) B
 ) A
