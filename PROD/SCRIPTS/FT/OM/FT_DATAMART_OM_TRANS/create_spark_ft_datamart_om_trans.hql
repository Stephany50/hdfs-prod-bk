CREATE TABLE MON.SPARK_FT_DATAMART_OM_TRANS
(
    MSISDN VARCHAR(100),
    MSISDN_BENEFICIAIRE VARCHAR(100),
    SERVICE VARCHAR(100),
    NB_SERVICES_DISTINCTS VARCHAR(100),
    MONTANT_TRANSACTION int,
    FRAIS_TRANSACTION int,
    INSERT_DATE timestamp

)
PARTITIONED BY(PERIOD DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")




SELECT COUNT(*) FROM
(
    SELECT
        DW.*
    FROM BACKUP_DWH.FT_DATAMART_OM_DAILY DW
    LEFT JOIN MON.SPARK_FT_DATAMART_OM_DAILY DL
    ON (
        NVL(DW.MSISDN, 'random') = NVL(DL.MSISDN, 'random')
        AND NVL(DW.USER_ID, 'random') = NVL(DL.USER_ID, 'random')
        AND NVL(cast(DW.NB_OPERATIONS as double), -999.9) = NVL(cast(DL.NB_OPERATIONS as double), -999.9)
        AND NVL(cast(DW.NBRE_TEL_SORTANT as int), -999) = NVL(cast(DL.NBRE_TEL_SORTANT as int), -999)
        AND NVL(cast(DW.DUREE_ENTRANT as double), -999.9) = NVL(cast(DL.DUREE_ENTRANT as double), -999.9)
        AND NVL(cast(DW.NBRE_TEL_ENTRANT as int), -999) = NVL(cast(DL.NBRE_TEL_ENTRANT as int), -999)
        AND NVL(cast(DW.NBRE_SMS_SORTANT as int), -999) = NVL(cast(DL.NBRE_SMS_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_SMS_ENTRANT as int), -999) = NVL(cast(DL.NBRE_SMS_ENTRANT as int), -999)
        AND NVL(DW.IMEI, 'random') = NVL(DL.IMEI, 'random')
        AND NVL(DW.LAC_CORRECTED, 'random') = NVL(DL.LAC_CORRECTED, 'random')
        AND NVL(DW.SERVED_PARTY_LOCATION, 'random') = NVL(DL.SERVED_PARTY_LOCATION, 'random')
        AND NVL(cast(DW.NBRE_TEL_MTN_SORTANT as int), -999) = NVL(cast(DL.NBRE_TEL_MTN_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_TEL_CAMTEL_SORTANT as int), -999) = NVL(cast(DL.NBRE_TEL_CAMTEL_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_TEL_OCM_SORTANT as int), -999) = NVL(cast(DL.NBRE_TEL_OCM_SORTANT as int), -999)
        AND NVL(cast(DW.DUREE_TEL_MTN_SORTANT as double), -999.9) = NVL(cast(DL.DUREE_TEL_MTN_SORTANT as double), -999.9)
        AND NVL(cast(DW.DUREE_TEL_CAMTEL_SORTANT as double), -999.9) = NVL(cast(DL.DUREE_TEL_CAMTEL_SORTANT as double), -999.9)
        AND NVL(cast(DW.DUREE_TEL_OCM_SORTANT as double), -999.9) = NVL(cast(DL.DUREE_TEL_OCM_SORTANT as double), -999.9)
        AND NVL(cast(DW.NBRE_SMS_MTN_SORTANT as int), -999) = NVL(cast(DL.NBRE_SMS_MTN_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_SMS_CAMTEL_SORTANT as int), -999) = NVL(cast(DL.NBRE_SMS_CAMTEL_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_SMS_OCM_SORTANT as int), -999) = NVL(cast(DL.NBRE_SMS_OCM_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_SMS_ZEBRA_SORTANT as int), -999) = NVL(cast(DL.NBRE_SMS_ZEBRA_SORTANT as int), -999)
        AND NVL(cast(DW.NBRE_TEL_MTN_ENTRANT as int), -999) = NVL(cast(DL.NBRE_TEL_MTN_ENTRANT as int), -999)
        AND NVL(cast(DW.NBRE_TEL_CAMTEL_ENTRANT as int), -999) = NVL(cast(DL.NBRE_TEL_CAMTEL_ENTRANT as int), -999)
            )
    WHERE
        TO_DATE(DW.EVENT_DATE) = '2020-01-01'
        AND DL.EVENT_DATE IS NULL
) T



