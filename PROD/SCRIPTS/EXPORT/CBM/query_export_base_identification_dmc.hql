SELECT
    A.msisdn
    , B.genre
    , type_piece
    , numero_piece
    , date_expiration
    , date_naissance
    , statut_validation_boo
    , est_suspendu
    , CURRENT_TIMESTAMP() insert_date
    , '###SLICE_VALUE###' event_date
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