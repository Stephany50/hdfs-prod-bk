INSERT INTO AGG.SPARK_FT_A_REPORTING_INTERCO
SELECT
    (
        CASE A.FAISCEAU
        WHEN 'Camtel National' THEN 'CAMTEL'
        ELSE  A.FAISCEAU
        END
    ) OPERATEUR,
    (
        CASE A.TYPE_APPEL
        WHEN 'Entrant' THEN
        (
           CASE A.USAGE_APPEL
           WHEN 'Telephony' THEN
           (
              CASE A.TYPE_HEURE
              WHEN 'HEURE PLEINE' THEN B.NB_NUM_APPELLES
              WHEN 'HEURE CREUSE' THEN 0
              END
           )
           WHEN 'SMS' THEN 0
           END
        )

        WHEN 'Sortant' THEN
        (
           CASE A.USAGE_APPEL
           WHEN 'Telephony' THEN
           (
              CASE A.TYPE_HEURE
              WHEN 'HEURE PLEINE' THEN B.NB_NUM_APPELANTS
              WHEN 'HEURE CREUSE' THEN 0
              END
           )
           WHEN 'SMS' THEN 0
           END
        )
        END
    ) NBR_UNIQ_USER,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN 'VOIX'
        WHEN 'SMS' THEN 'SMS'
        END
    ) TYPE_SERVICE,
    UPPER(A.TYPE_APPEL) TYPE_APPEL,
    UPPER(A.TYPE_HEURE) TYPE_HEURE,
    SUM(NBRE_APPEL) NOMBRE_TRANSACTION,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN SUM(DUREE_APPEL)/60
        WHEN 'SMS' THEN 0
        END
    ) VOLUME,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN
        (
            CASE A.TYPE_APPEL
            WHEN 'Entrant' THEN (SUM(DUREE_APPEL) * C.TARIF)/60
            WHEN 'Sortant' THEN 0
            END
        )
        WHEN 'SMS' THEN
        (
            CASE A.TYPE_APPEL
            WHEN 'Entrant' THEN (SUM(NBRE_APPEL) * C.TARIF)
            WHEN 'Sortant' THEN 0
            END
        )
        END
    ) REVENU,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN
        (
            CASE A.TYPE_APPEL
            WHEN 'Entrant' THEN 0
            WHEN 'Sortant' THEN (SUM(DUREE_APPEL) * D.TARIF)/60
            END
        )
        WHEN 'SMS' THEN
        (
            CASE A.TYPE_APPEL
            WHEN 'Entrant' THEN 0
            WHEN 'Sortant' THEN (SUM(NBRE_APPEL) * D.TARIF)
            END
        )
        END
    ) CHARGE,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' PROCESSING_DATE
FROM
(
    SELECT
        FAISCEAU,
        USAGE_APPEL,
        TYPE_HEURE,
        TYPE_APPEL,
        SUM(DUREE_APPEL) DUREE_APPEL,
        SUM(NBRE_APPEL) NBRE_APPEL
    FROM AGG.SPARK_FT_X_INTERCO_FINAL WHERE SDATE='###SLICE_VALUE###'
    AND FAISCEAU IN ('VIETTEL', 'Camtel National', 'MTN') AND
      TYPE_APPEL IN ('Entrant', 'Sortant') AND
      USAGE_APPEL IN ('Telephony', 'SMS')
    GROUP BY FAISCEAU,
        USAGE_APPEL,
        TYPE_HEURE,
        TYPE_APPEL
) A
LEFT JOIN
(
    SELECT
        *
     FROM AGG.SPARK_FT_A_SUBS_SPHERE_TRAFFIC_DAY
     WHERE EVENT_DATE='###SLICE_VALUE###'
) B
ON (
        CASE A.FAISCEAU
        WHEN 'Camtel National' THEN 'CAMTEL'
        ELSE  A.FAISCEAU
        END
    )=B.OPERATEUR
LEFT JOIN
(SELECT DISTINCT DATE_DEBUT, DATE_FIN, USAGE, TYPE_HEURE, TARIF FROM DIM.SPARK_DT_TARRIF_INTERCO) C
ON
(CASE A.TYPE_HEURE
    WHEN 'HEURE PLEINE' THEN 'HP'
    WHEN 'HEURE CREUSE' THEN 'HC'
END) = C.TYPE_HEURE
AND
(CASE A.USAGE_APPEL
    WHEN 'Telephony' THEN 'VOIX'
    WHEN 'SMS' THEN 'SMS'
END)=C.USAGE
LEFT JOIN
(SELECT DISTINCT DATE_DEBUT, DATE_FIN, USAGE, TYPE_HEURE, OPERATEUR, TARIF FROM DIM.SPARK_DT_TARRIF_INTERCO_SORTANT) D
ON
(CASE A.TYPE_HEURE
    WHEN 'HEURE PLEINE' THEN 'HP'
    WHEN 'HEURE CREUSE' THEN 'HC'
END) = D.TYPE_HEURE
AND
(CASE A.USAGE_APPEL
    WHEN 'Telephony' THEN 'VOIX'
    WHEN 'SMS' THEN 'SMS'
END)=D.USAGE
AND
(
    CASE A.FAISCEAU
    WHEN 'Camtel National' THEN 'CAMTEL'
    ELSE  A.FAISCEAU
    END
)=D.OPERATEUR
WHERE '###SLICE_VALUE###' BETWEEN C.DATE_DEBUT AND C.DATE_FIN
    AND '###SLICE_VALUE###' BETWEEN D.DATE_DEBUT AND D.DATE_FIN
GROUP BY
    A.FAISCEAU,
    A.USAGE_APPEL,
    A.TYPE_HEURE,
    A.TYPE_APPEL,
    C.TARIF,
    D.TARIF,
    B.NB_NUM_APPELLES,
    B.NB_NUM_APPELANTS