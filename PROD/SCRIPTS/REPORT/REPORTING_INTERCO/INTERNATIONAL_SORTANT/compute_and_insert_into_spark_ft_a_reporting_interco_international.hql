INSERT INTO AGG.SPARK_FT_A_REPORTING_INTERCO_INTERNATIONAL
SELECT
    SUBSTR('###SLICE_VALUE###', 1, 7)MONTH, -- ok
    TYPE_ABONNE USER_TYPE, -- ok
    FAISCEAU CARRIER, -- ok
    B.NAME COUNTRY,
    OPERATEUR OPERATOR,
    USAGE_APPEL SERVICE_TYPE, -- ok
    TYPE_APPEL CALL_DIRECTION, -- ok
    SUM(NBRE_APPEL) NBR_TRANSACTION,
    NVL(SUM(DUREE_APPEL), 0)/60 CALL_DURATION,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' PROCESSING
FROM
(
    SELECT
        MSISDN,
        FAISCEAU,
        USAGE_APPEL,
        TYPE_APPEL,
        TYPE_ABONNE,
        OPERATEUR,
--         RANG_OP,
        SUM(DUREE_APPEL) DUREE_APPEL,--
        SUM(NBRE_APPEL) NBRE_APPEL
    FROM
    (
        SELECT
            MSISDN,
            FAISCEAU,
            USAGE_APPEL,
            TYPE_APPEL,
            TYPE_ABONNE,
            C.CODE_OPERATEUR CODE_OPERATEUR,
            C.OPERATEUR OPERATEUR,
            (ROW_NUMBER() OVER(PARTITION BY MSISDN, FAISCEAU, USAGE_APPEL, TYPE_APPEL, TYPE_ABONNE, C.CODE_OPERATEUR ORDER BY LENGTH(UPPER(TRIM(C.CODE_OPERATEUR))) DESC NULLS LAST)) AS RANG_OP,
            SUM(DUREE_APPEL) DUREE_APPEL,--
            SUM(NBRE_APPEL) NBRE_APPEL
        FROM
        (
            SELECT
                (
                    CASE
                        WHEN TYPE_APPEL = 'Sortant' THEN OTHER_PARTY
                        WHEN TYPE_APPEL = 'Entrant' THEN SERVED_PARTY
                    END
                ) MSISDN,
                FAISCEAU,
                USAGE_APPEL,
                TYPE_APPEL,
                TYPE_ABONNE,
        --         ROW_NUMBER OVER(PARTITION BY ),
                SUM(DUREE_APPEL) DUREE_APPEL,
                SUM(NBRE_APPEL) NBRE_APPEL
            FROM TMP.SPARK_FT_X_INTERCO_FINAL_2 WHERE SDATE='###SLICE_VALUE###'
            AND FAISCEAU IN ('BELG', 'Orange CI', 'FTLD') AND
                TYPE_APPEL IN ('Entrant', 'Sortant') AND
                USAGE_APPEL IN ('Telephony', 'SMS')
            GROUP BY FAISCEAU,
                USAGE_APPEL,
                TYPE_APPEL,
                TYPE_ABONNE,
                (
                    CASE
                        WHEN TYPE_APPEL = 'Sortant' THEN OTHER_PARTY
                        WHEN TYPE_APPEL = 'Entrant' THEN SERVED_PARTY
                    END
                )
       ) Y
       LEFT JOIN DIM.SPARK_DT_REF_OPERATEURS C
        ON SUBSTR(Y.MSISDN,1,LENGTH(TRIM(C.CODE_OPERATEUR))) = TRIM(C.CODE_OPERATEUR)
       GROUP BY FAISCEAU,
        USAGE_APPEL,
        TYPE_APPEL,
        TYPE_ABONNE,
        MSISDN,
        C.CODE_OPERATEUR,
        C.OPERATEUR
   ) Z
   WHERE RANG_OP = 1
   GROUP BY FAISCEAU,
        USAGE_APPEL,
        TYPE_APPEL,
        TYPE_ABONNE,
        MSISDN,
        OPERATEUR
--     order by length(upper(trim(D.code_operateur))) DESC nulls last) as RANG_OP
) A
LEFT JOIN
(
    SELECT
        CODE,
        NAME
    FROM
    (
        SELECT
             UPPER(TRIM(NAME)) AS NAME,
             UPPER(TRIM(CODE)) AS CODE,
             ROW_NUMBER() OVER(PARTITION BY UPPER(TRIM(CODE)) ORDER BY LENGTH(UPPER(TRIM(NAME))) DESC NULLS LAST) AS RANG
        FROM DIM.SPARK_DT_COUNTRIES
    ) A
    WHERE RANG = 1
) B
ON SUBSTR(TRIM(A.MSISDN),1,LENGTH(TRIM(CODE))) = TRIM(B.CODE)
-- LEFT JOIN DIM.SPARK_DT_REF_OPERATEURS C
-- ON SUBSTR(A.MSISDN,1,LENGTH(TRIM(C.CODE_OPERATEUR))) = TRIM(C.CODE_OPERATEUR)
-- WHERE RANG_OP = 1

GROUP BY A.FAISCEAU,
    A.USAGE_APPEL,
    A.TYPE_APPEL,
    A.TYPE_ABONNE,
    A.OPERATEUR,
    B.NAME









