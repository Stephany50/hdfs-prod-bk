CREATE TABLE MON.SPARK_FT_IMEI_TRANSACTION
(
    IMEI VARCHAR(40),
    MSISDN VARCHAR(40),
    SITE_VOIX VARCHAR(40),
    SITE_DATA VARCHAR(40),
    TECHNOLOGIE VARCHAR(40),
    TERMINAL_TYPE VARCHAR(40),
    MSISDN_COUNT DOUBLE,
    NOMBRE_TRANSACTIONS_ENTRANT DOUBLE,
    NOMBRE_TRANSACTIONS_SORTANT DOUBLE,
    DUREE_ENTRANT DOUBLE,
    DUREE_SORTANT DOUBLE,
    VOLUME_DATA_GPRS DOUBLE,
    VOLUME_DATA_GPRS_2G DOUBLE,
    VOLUME_DATA_GPRS_3G DOUBLE,
    VOLUME_DATA_GPRS_4G DOUBLE,
    VOLUME_DATA_OTARIE DOUBLE,
    VOLUME_DATA_OTARIE_2G DOUBLE,
    VOLUME_DATA_OTARIE_3G DOUBLE,
    VOLUME_DATA_OTARIE_4G DOUBLE,
    SRC_TABLE VARCHAR(100),
    INSERT_DATE timestamp

)
PARTITIONED BY(EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")









SELECT COUNT(*) FROM
(
    SELECT
        DW.*
    FROM BACKUP_DWH.FT_IMEI_TRANSACTION DW
    LEFT JOIN BACKUP_DWH.FT_IMEI_TRANSACTION_TMP  DL
    ON (
        NVL(DW.IMEI, 'random') = NVL(DL.IMEI, 'random')
        AND NVL(DW.MSISDN, 'random') = NVL(DL.MSISDN, 'random')
        AND NVL(DW.SITE_VOIX, 'random') = NVL(DL.MSISDN, 'random')
        AND NVL(DW.SITE_DATA, 'random') = NVL(DL.SITE_DATA, 'random')
--         AND NVL(DW.TECHNOLOGIE, 'random') = NVL(DL.TECHNOLOGIE, 'random')
--         AND NVL(DW.TERMINAL_TYPE, 'random') = NVL(DL.TERMINAL_TYPE, 'random')
--         AND NVL(cast(DW.MSISDN_COUNT as double), -999.9) = NVL(cast(DL.MSISDN_COUNT as double), -999.9)
--         AND NVL(cast(DW.NOMBRE_TRANSACTIONS_ENTRANT as double), -999.9) = NVL(cast(DL.NOMBRE_TRANSACTIONS_ENTRANT as double), -999.9)
--         AND NVL(cast(DW.NOMBRE_TRANSACTIONS_SORTANT as double), -999.9) = NVL(cast(DL.NOMBRE_TRANSACTIONS_SORTANT as double), -999.9)
--         AND NVL(cast(DW.DUREE_ENTRANT as double), -999.9) = NVL(cast(DL.DUREE_ENTRANT as double), -999.9)
--         AND NVL(cast(DW.DUREE_SORTANT as double), -999.9) = NVL(cast(DL.DUREE_SORTANT as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_GPRS as double), -999.9) = NVL(cast(DL.VOLUME_DATA_GPRS as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_GPRS_2G as double), -999.9) = NVL(cast(DL.VOLUME_DATA_GPRS_2G as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_GPRS_3G as double), -999.9) = NVL(cast(DL.VOLUME_DATA_GPRS_3G as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_GPRS_4G as double), -999.9) = NVL(cast(DL.VOLUME_DATA_GPRS_4G as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_OTARIE as double), -999.9) = NVL(cast(DL.VOLUME_DATA_OTARIE as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_OTARIE_2G as double), -999.9) = NVL(cast(DL.VOLUME_DATA_OTARIE_2G as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_OTARIE_3G as double), -999.9) = NVL(cast(DL.VOLUME_DATA_OTARIE_3G as double), -999.9)
--         AND NVL(cast(DW.VOLUME_DATA_OTARIE_4G as double), -999.9) = NVL(cast(DL.VOLUME_DATA_OTARIE_4G as double), -999.9)
--         AND NVL(DW.SRC_TABLE, 'random') = NVL(DL.SRC_TABLE, 'random')
        )
    WHERE
        TO_DATE(DW.EVENT_DATE) = '2020-02-05'
        AND DL.EVENT_DATE IS NULL
) T