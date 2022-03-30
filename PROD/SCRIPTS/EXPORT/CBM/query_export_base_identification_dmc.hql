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
        , STATUT_VALIDATION_BO STATUT_VALIDATION_BOO
        , STATUT_HLR EST_SUSPENDU
    FROM CDR.SPARK_IT_KYC_BDI_FULL 
    WHERE ORIGINAL_FILE_DATE IN (SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.SPARK_IT_KYC_BDI_FULL)
)  A
LEFT JOIN DIM.SPARK_DT_BASE_IDENTIFICATION B
ON A.MSISDN = B.MSISDN