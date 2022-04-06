INSERT INTO TMP.TT_SPARK_FT_CLIENT_LAST_CALLED_90_DAYS
SELECT
    A.CALLER,
    CONCAT_WS('|', COLLECT_SET(A.CALLEE)) CALLEE_LIST,
    COUNT(DISTINCT A.CALLEE) NBR_CALLEE,
    MAX(SITE_NAME) SITE_NAME
FROM
(
    SELECT
        (
            CASE WHEN UPPER(TRANSACTION_DIRECTION)='SORTANT' THEN SERVED_MSISDN
            ELSE OTHER_PARTY
            END
        ) CALLER,
        (
            CASE WHEN UPPER(TRANSACTION_DIRECTION)='ENTRANT' THEN SERVED_MSISDN
            ELSE OTHER_PARTY
            END
        ) CALLEE
    FROM MON.SPARK_FT_MSC_TRANSACTION
    WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE, 92s) AND DATE_SUB(CURRENT_DATE, 3) 
        AND LENGTH(SERVED_MSISDN)=9 
        AND LENGTH(OTHER_PARTY)=9 
        AND UPPER(TRANSACTION_TYPE) LIKE "TEL%"
    GROUP BY
        (
            CASE WHEN UPPER(TRANSACTION_DIRECTION)='SORTANT' THEN SERVED_MSISDN
            ELSE OTHER_PARTY
            END
        ),
        (
            CASE WHEN UPPER(TRANSACTION_DIRECTION)='ENTRANT' THEN SERVED_MSISDN
            ELSE OTHER_PARTY
            END
        )
) A 
LEFT JOIN
(
    SELECT 
        MSISDN, 
        MAX(SITE_NAME) SITE_NAME
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
    WHERE EVENT_DATE=DATE_SUB(CURRENT_DATE(), 3) 
    GROUP BY MSISDN
) B
ON A.CALLER=B.MSISDN
WHERE FN_NNP_SIMPLE_DESTINATION(CALLER)='OCM'
GROUP BY CALLER