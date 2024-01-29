INSERT INTO MON.SPARK_FT_PROCHAINE_EXPIRATION_CNI
    SELECT
        type_piece,
        date_expiration,
        COUNT(*) AS nombre_type,
        current_timestamp AS insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM MON.SPARK_FT_KYC_BDI_PP
    WHERE event_date = '###SLICE_VALUE###' and est_suspendu ='NON'
    GROUP BY
        type_piece,
        date_expiration