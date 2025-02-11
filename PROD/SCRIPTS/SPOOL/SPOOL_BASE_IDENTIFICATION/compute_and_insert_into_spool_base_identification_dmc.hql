INSERT INTO SPOOL.SPOOL_BASE_IDENTIFICATION_DMC
SELECT
    A.MSISDN
    , B.GENRE
    , TYPE_PIECE
    , NUMERO_PIECE
    , DATE_EXPIRATION
    , DATE_NAISSANCE
    , STATUT_VALIDATION_BOO
    , EST_SUSPENDU
    , CURRENT_TIMESTAMP() INSERT_DATE
    , '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        MSISDN
        , TYPE_PIECE
        , NUMERO_PIECE
        , DATE_EXPIRATION
        , DATE_NAISSANCE
        , STATUT_VALIDATION_BOO
        , EST_SUSPENDU
    FROM MON.SPARK_FT_BDI
    WHERE EVENT_DATE IN (SELECT MAX(EVENT_DATE) FROM MON.SPARK_FT_BDI)
)  A
RIGHT JOIN DIM.SPARK_DT_BASE_IDENTIFICATION B
ON A.MSISDN = B.MSISDN