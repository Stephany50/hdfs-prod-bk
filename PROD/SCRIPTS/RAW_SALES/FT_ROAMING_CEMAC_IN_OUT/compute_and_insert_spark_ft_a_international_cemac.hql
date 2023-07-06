INSERT INTO AGG.SPARK_FT_INTERNATIONAL_CEMAC PARTITION(CALL_DATE)

SELECT
    'CMR02' CLIENT_OPERATOR,
    TYPE_APPEL DIRECTION,
    OPERATEUR ROAMING_PARTNER_NAME,
    FIRST(B.NAME) ROAMING_PARTNER_COUNTRY,
    'Voice' SERVICES_FAMILY,
    SUM(NBRE_APPEL) NUMBER_OF_CALLS,
    NVL(SUM(DUREE_APPEL), 0)/60 MINUTES,
    SERVED_IMSI IMSI,
    '###SLICE_VALUE###' CALL_DATE
FROM
(
    SELECT
        SERVED_IMSI,
        MSISDN,
        USAGE_APPEL,
        TYPE_APPEL,
        TYPE_ABONNE,
        OPERATEUR,
        SUM(DUREE_APPEL) DUREE_APPEL,
        SUM(NBRE_APPEL) NBRE_APPEL
    FROM
    (
        SELECT
            SERVED_IMSI,
            MSISDN,
            USAGE_APPEL,
            TYPE_APPEL,
            TYPE_ABONNE,
            C.CODE_OPERATEUR CODE_OPERATEUR,
            C.OPERATEUR OPERATEUR,
            (ROW_NUMBER() OVER(PARTITION BY SERVED_IMSI,MSISDN, USAGE_APPEL, TYPE_APPEL, TYPE_ABONNE, DUREE_APPEL, NBRE_APPEL  ORDER BY LENGTH(UPPER(TRIM(C.CODE_OPERATEUR))) DESC NULLS LAST)) AS RANG_OP,
            DUREE_APPEL,
            NBRE_APPEL
        FROM
        (
            SELECT
                (
                    CASE
                        WHEN TYPE_APPEL = 'Sortant' THEN OTHER_PARTY
                        WHEN TYPE_APPEL = 'Entrant' THEN SERVED_PARTY
                    END
                ) MSISDN,
                SERVED_IMSI,
                USAGE_APPEL,
                TYPE_APPEL,
                TYPE_ABONNE,
                SUM(DUREE_APPEL) DUREE_APPEL,
                SUM(NBRE_APPEL) NBRE_APPEL
            FROM TMP.SPARK_FT_X_INTERCO_FINAL_2_CEMAC WHERE SDATE='###SLICE_VALUE###'
            AND FAISCEAU IN ('BELG', 'Orange CI', 'FTLD') AND
                TYPE_APPEL IN ('Entrant', 'Sortant') AND
                USAGE_APPEL IN ('Telephony', 'SMS')
            GROUP BY SERVED_IMSI, 
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
       INNER JOIN (SELECT * FROM DIM.SPARK_DT_REF_OPERATEURS  WHERE cc in (235,236,237,240,241,242) and code_operateur not in ('235','236','237','240','241','242') )  C
        ON SUBSTR(Y.MSISDN,1,LENGTH(TRIM(C.CODE_OPERATEUR))) = TRIM(C.CODE_OPERATEUR)
   ) Z
   WHERE RANG_OP = 1
   GROUP BY SERVED_IMSI,
        USAGE_APPEL,
        TYPE_APPEL,
        TYPE_ABONNE,
        MSISDN,
        OPERATEUR
) A

LEFT JOIN
(
    SELECT
        CODE,
        NAME
    FROM
    (
        SELECT
             UPPER(TRIM(MAX(P.NAME))) AS NAME,
             UPPER(TRIM(P.CODE)) AS CODE
        FROM ( SELECT * FROM DIM.SPARK_DT_COUNTRIES WHERE code in (235,236,237,240,241,242) )P
        GROUP BY CODE
    ) A

) B

ON SUBSTR(TRIM(A.MSISDN),1,LENGTH(TRIM(CODE))) = TRIM(B.CODE)
GROUP BY A.SERVED_IMSI,
    A.USAGE_APPEL,
    A.TYPE_APPEL,
    B.NAME, 
    A.OPERATEUR


          






    


