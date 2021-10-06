INSERT  INTO MON.SPARK_TMP_OG_IC_CALL_SNAPSHOT_PRE
SELECT
        T.MSISDN,
        MAX(T.OG_CALL) OG_CALL,
        MAX(T.IC_CALL_1) IC_CALL_1,
        MAX(T.IC_CALL_2) IC_CALL_2,
        MAX(T.IC_CALL_3) IC_CALL_3,
        MAX(T.IC_CALL_4) IC_CALL_4
FROM
(
        SELECT
                S.MSISDN MSISDN,
                S.OG_CALL OG_CALL,
                IF (IC_CALL_COUNT >= 4,S.IC_CALL, NULL) IC_CALL_1,
                IF (IC_CALL_COUNT >= 3,S.IC_CALL, NULL) IC_CALL_2,
                IF (IC_CALL_COUNT >= 2,S.IC_CALL, NULL) IC_CALL_3,
                IF (IC_CALL_COUNT >= 1,S.IC_CALL, NULL) IC_CALL_4
        FROM
        (
                SELECT
                        (
                        CASE WHEN LENGTH(SERVED_MSISDN) = 13 AND substr(SERVED_MSISDN,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(SERVED_MSISDN) IN ('MTN','VIETTEL','OCM')
                        THEN SUBSTR(SERVED_MSISDN,-9)
                        ELSE SERVED_MSISDN
                        END
                        ) MSISDN,
                        MAX (CASE
                        WHEN TRANSACTION_DIRECTION = 'Sortant'  THEN TRANSACTION_DATE
                        ELSE NULL
                        END)  OG_CALL,
                        MAX (CASE
                        WHEN TRANSACTION_DIRECTION = 'Entrant'  THEN TRANSACTION_DATE
                        ELSE NULL
                        END) IC_CALL,
                        COUNT (
                        DISTINCT (CASE
                        WHEN
                        TRANSACTION_DIRECTION = 'Entrant' THEN CONCAT(transaction_date,transaction_time)
                        ELSE NULL
                        END)
                        ) IC_CALL_COUNT
                FROM
                        MON.SPARK_FT_MSC_TRANSACTION a
                WHERE
                        TRANSACTION_DATE = DATE_SUB('###SLICE_VALUE###',1)
                        AND (CASE
                        WHEN OLD_CALLING_NUMBER IS NULL THEN 1
                        WHEN OLD_CALLING_NUMBER IN ('23799900929', '99900929','237699900929','699900929') THEN 0
                        WHEN OLD_CALLING_NUMBER rlike '^[\+]*[0-9]+$'   THEN 1
                        ELSE 0
                        END) = 1
                        AND OTHER_PARTY NOT IN ('937','938','924')
                        AND FN_NNP_SIMPLE_DESTINATION (SERVED_MSISDN) IN ('OCM','SET')
                GROUP BY
                        (CASE WHEN LENGTH(SERVED_MSISDN) = 13 AND substr(SERVED_MSISDN,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(SERVED_MSISDN)
                        IN ('MTN','VIETTEL','OCM') THEN SUBSTR(SERVED_MSISDN,-9) ELSE SERVED_MSISDN END)
        ) S
        WHERE
                S.MSISDN IS NOT NULL
        UNION
        SELECT
                fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN) MSISDN,
                MAX(TRANSACTION_DATE) OG_CALL,
                NULL IC_CALL_1 ,
                NULL IC_CALL_2,
                NULL IC_CALL_3,
                NULL IC_CALL_4
        FROM
                MON.SPARK_FT_SUBSCRIPTION
        WHERE
                TRANSACTION_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',3) and DATE_SUB('###SLICE_VALUE###',1)
                AND RATED_AMOUNT > 0
        GROUP BY
                fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN)
        UNION
        select
                SERVED_PARTY_MSISDN msisdn,
                MAX(session_DATE)  OG_CALL,
                NULL IC_CALL_1 ,
                NULL IC_CALL_2,
                NULL IC_CALL_3,
                NULL IC_CALL_4
        from
                MON.SPARK_ft_cra_gprs
        where
                session_date BETWEEN DATE_SUB('###SLICE_VALUE###',3) and DATE_SUB('###SLICE_VALUE###',1)
                and bytes_sent + bytes_received + total_cost + session_duration > 0
                and SERVED_PARTY_MSISDN is not null
        group by SERVED_PARTY_MSISDN
) T
GROUP BY T.MSISDN