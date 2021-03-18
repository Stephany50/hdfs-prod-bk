INSERT INTO AGG.SPARK_FT_A_REPORTING_360
SELECT
    NVL(B.REGION, 'INCONNU') ADMINISTRATIVE_REGION,
    (
        CASE B.REGION
        WHEN 'SUD' THEN 'CENTRE - SUD - EST'
        WHEN 'CENTRE' THEN 'CENTRE - SUD - EST'
        WHEN 'SUD-OUEST' THEN 'LITTORAL - SUD-OUEST'
        WHEN 'ADAMAOUA' THEN 'GRAND NORD'
        WHEN 'LITTORAL' THEN 'LITTORAL - SUD-OUEST'
        WHEN 'EXTREME-NORD' THEN 'GRAND NORD'
        WHEN 'NORD-OUEST' THEN 'OUEST - NORD-OUEST'
        WHEN 'EST' THEN 'CENTRE - SUD - EST'
        WHEN 'OUEST' THEN 'OUEST - NORD-OUEST'
        WHEN 'NORD' THEN 'GRAND NORD'
        ELSE 'INCONNU'
        END
    ) COMMERCIAL_REGION,
    'INCOMING TRAFFIC' KPI_GROUP_NAME,
    -- (
    --     CASE A.USAGE_APPEL
    --     WHEN 'Telephony' THEN 'INCOMING VOICE TRAFFIC'
    --     WHEN 'SMS' THEN 'INCOMING SMS TRAFFIC'
    --     END
    -- ) KPI_GROUP_NAME,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN
            (
                CASE A.FAISCEAU
                WHEN 'VIETTEL' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'NEXTTEL INCOMING VOICE HP'
                    WHEN 'HEURE CREUSE' THEN 'NEXTTEL INCOMING VOICE HC'
                    END
                )
                WHEN 'Camtel National' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'CAMTEL INCOMING VOICE HP'
                    WHEN 'HEURE CREUSE' THEN 'CAMTEL INCOMING VOICE HC'
                    END
                )
                WHEN 'MTN' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'MTN INCOMING VOICE HP'
                    WHEN 'HEURE CREUSE' THEN 'MTN INCOMING VOICE HC'
                    END
                )
                ELSE
                    CONCAT_WS(' ', A.FAISCEAU, 'INCOMING VOICE')
                END
            )
        WHEN 'SMS' THEN
            (
                CASE A.FAISCEAU
                WHEN 'VIETTEL' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'NEXTTEL INCOMING SMS HP'
                    WHEN 'HEURE CREUSE' THEN 'NEXTTEL INCOMING SMS HC'
                    END
                )
                WHEN 'Camtel National' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'CAMTEL INCOMING SMS HP'
                    WHEN 'HEURE CREUSE' THEN 'CAMTEL INCOMING SMS HC'
                    END
                )
                WHEN 'MTN' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'MTN INCOMING SMS HP'
                    WHEN 'HEURE CREUSE' THEN 'MTN INCOMING SMS HC'
                    END
                )
                ELSE
                    CONCAT_WS(' ', A.FAISCEAU, 'INCOMING SMS')
                END
            )
        END
    ) KPI_NAME,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN SUM(DUREE_APPEL)
        WHEN 'SMS' THEN SUM(NBRE_APPEL)
        END
    ) VALUE,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' PROCESSING_DATE
FROM
(
    SELECT * FROM AGG.SPARK_FT_X_INTERCO_FINAL WHERE SDATE='###SLICE_VALUE###'
) A
LEFT JOIN
(SELECT CI, LAC, MAX(TRIM(REGION_TERRITORIALE)) REGION FROM DIM.SPARK_DT_GSM_CELL_CODE_MKT GROUP BY CI, LAC) B
ON
SUBSTR(TRIM(A.MSC_LOCATION), 8, 5) = LPAD(TRIM(B.LAC),5,0) AND SUBSTR(TRIM(A.MSC_LOCATION), -5) = LPAD(TRIM(B.CI),5,0)
WHERE A.FAISCEAU NOT IN ('Orange Cameroun') AND
      A.TYPE_APPEL='Entrant' AND
      A.USAGE_APPEL IN ('Telephony', 'SMS')
GROUP BY A.FAISCEAU,
         A.USAGE_APPEL,
         A.TYPE_HEURE,
         B.REGION

UNION ALL

SELECT
    NVL(B.REGION, 'INCONNU') ADMINISTRATIVE_REGION,
    (
        CASE B.REGION
        WHEN 'SUD' THEN 'CENTRE - SUD - EST'
        WHEN 'CENTRE' THEN 'CENTRE - SUD - EST'
        WHEN 'SUD-OUEST' THEN 'LITTORAL - SUD-OUEST'
        WHEN 'ADAMAOUA' THEN 'GRAND NORD'
        WHEN 'LITTORAL' THEN 'LITTORAL - SUD-OUEST'
        WHEN 'EXTREME-NORD' THEN 'GRAND NORD'
        WHEN 'NORD-OUEST' THEN 'OUEST - NORD-OUEST'
        WHEN 'EST' THEN 'CENTRE - SUD - EST'
        WHEN 'OUEST' THEN 'OUEST - NORD-OUEST'
        WHEN 'NORD' THEN 'GRAND NORD'
        ELSE 'INCONNU'
        END
    ) COMMERCIAL_REGION,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN 'INCOMING REVENU'
        WHEN 'SMS' THEN 'INCOMING REVENU'
        END
    ) KPI_GROUP_NAME,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN
            (
                CASE A.FAISCEAU
                WHEN 'VIETTEL' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'NEXTTEL INCOMING VOICE REVENU HP'
                    WHEN 'HEURE CREUSE' THEN 'NEXTTEL INCOMING VOICE REVENU HC'
                    END
                )
                WHEN 'Camtel National' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'CAMTEL INCOMING VOICE REVENU HP'
                    WHEN 'HEURE CREUSE' THEN 'CAMTEL INCOMING VOICE REVENU HC'
                    END
                )
                WHEN 'MTN' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'MTN INCOMING VOICE REVENU HP'
                    WHEN 'HEURE CREUSE' THEN 'MTN INCOMING VOICE REVENU HC'
                    END
                )
                END
            )
        WHEN 'SMS' THEN
            (
                CASE A.FAISCEAU
                WHEN 'VIETTEL' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'NEXTTEL INCOMING SMS REVENU HP'
                    WHEN 'HEURE CREUSE' THEN 'NEXTTEL INCOMING SMS REVENU HC'
                    END
                )
                WHEN 'Camtel National' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'CAMTEL INCOMING SMS REVENU HP'
                    WHEN 'HEURE CREUSE' THEN 'CAMTEL INCOMING SMS REVENU HC'
                    END
                )
                WHEN 'MTN' THEN
                (
                    CASE A.TYPE_HEURE
                    WHEN 'HEURE PLEINE' THEN 'MTN INCOMING SMS REVENU HP'
                    WHEN 'HEURE CREUSE' THEN 'MTN INCOMING SMS REVENU HC'
                    END
                )
                END
            )
        END
    ) KPI_NAME,
    (
        CASE A.USAGE_APPEL
        WHEN 'Telephony' THEN (SUM(DUREE_APPEL) * C.TARIF)/60
        WHEN 'SMS' THEN (SUM(NBRE_APPEL) * C.TARIF)
        END
    ) VALUE,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' PROCESSING_DATE
FROM
(
    SELECT * FROM AGG.SPARK_FT_X_INTERCO_FINAL WHERE SDATE='###SLICE_VALUE###'
) A
LEFT JOIN
(SELECT CI, LAC, MAX(TRIM(REGION_TERRITORIALE)) REGION FROM DIM.SPARK_DT_GSM_CELL_CODE_MKT GROUP BY CI, LAC) B
ON
SUBSTR(TRIM(A.MSC_LOCATION), 8, 5) = LPAD(TRIM(B.LAC),5,0) AND SUBSTR(TRIM(A.MSC_LOCATION), -5) = LPAD(TRIM(B.CI),5,0)
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
WHERE A.FAISCEAU IN ('VIETTEL', 'Camtel National', 'MTN') AND
      A.TYPE_APPEL='Entrant' AND
      A.USAGE_APPEL IN ('Telephony', 'SMS') AND
      '###SLICE_VALUE###' BETWEEN C.DATE_DEBUT AND C.DATE_FIN
GROUP BY A.FAISCEAU,
         A.USAGE_APPEL,
         A.TYPE_HEURE,
         B.REGION,
         C.TARIF