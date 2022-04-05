INSERT INTO TMP.TT_SPARK_FT_MULTI_SIM
SELECT
    MSISDN_A,
    MSISDN_B,
    SCORE,
    NBR_CALLEE_A,
    NBR_CALLEE_B
FROM
(
    SELECT
        A1.CALLER MSISDN_A,
        A2.CALLER MSISDN_B,
        FN_MULTISIM_SCORE(
            NVL(A1.CALLER, ''), 
            NVL(A2.CALLER, ''), 
            NVL(A1.CALLEE_LIST, ''), 
            NVL(A2.CALLEE_LIST, '')
        ) SCORE,
        A1.NBR_CALLEE NBR_CALLEE_A,
        A2.NBR_CALLEE NBR_CALLEE_B
    FROM
    (
        SELECT 
            CALLER,
            CALLEE_LIST,
            NBR_CALLEE,
            SITE_NAME
        FROM 
        TMP.TT_SPARK_FT_CLIENT_LAST_CALLED_90_DAYS_FILTERED 
    ) A1
    LEFT JOIN
    (
        SELECT 
            CALLER,
            CALLEE_LIST,
            NBR_CALLEE,
            SITE_NAME
        FROM 
        TMP.TT_SPARK_FT_CLIENT_LAST_CALLED_90_DAYS_FILTERED 
    ) A2
    ON A1.SITE_NAME=A2.SITE_NAME 
    AND A1.CALLER<>A2.CALLER
) A
WHERE SCORE > 0.7
    