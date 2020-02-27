CREATE TABLE MON.SPARK_FT_DATAMART_OM_DAILY
(
    MSISDN VARCHAR(100),
    USER_ID VARCHAR(100),
    NB_OPERATIONS DOUBLE,
    NB_SERVICES_DISTINCTS DOUBLE,
    SOLDE_FIN_JOURNEE DOUBLE,
    ARPU_OM DOUBLE,
    NB_CI DOUBLE,
    MONTANT_CI DOUBLE,
    NB_CO DOUBLE,
    MONTANT_CO DOUBLE,
    FRAIS_CO DOUBLE,
    NB_BILL_PAY DOUBLE,
    MONTANT_BILL_PAY DOUBLE,
    FRAIS_BILL_PAY DOUBLE,
    NB_MERCHPAY DOUBLE,
    MONTANT_MERCHPAY DOUBLE,
    FRAIS_MERCHPAY DOUBLE,
    MONTANT_P2P_RECU DOUBLE,
    FRAIS_P2P_ORANGE DOUBLE,
    NB_P2P_RECU DOUBLE,
    MONTANT_P2P_ORANGE DOUBLE,
    MONTANT_TNO DOUBLE,
    FRAIS_TNO DOUBLE,
    NB_TOP_UP DOUBLE,
    MONTANT_TOP_UP DOUBLE,
    NB_AUTRES DOUBLE,
    MONTANT_AUTRES DOUBLE,
    NB_BUNDLES_DATA DOUBLE,
    MONTANT_BDLE_DATA DOUBLE,
    NB_BUNDLE_VOIX DOUBLE,
    MONTANT_BDLE_VOIX DOUBLE,
    INSERT_DATE timestamp

)
PARTITIONED BY(PERIOD DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")



SELECT * FROM
(
    SELECT
        DW.*
    FROM BACKUP_DWH.FT_DATAMART_OM_DAILY DW
    LEFT JOIN MON.SPARK_FT_DATAMART_OM_DAILY DL
    ON (
        NVL(DW.MSISDN, 'random') = NVL(DL.MSISDN, 'random')
        AND NVL(DW.USER_ID, 'random') = NVL(DL.USER_ID, 'random')
        AND NVL(cast(DW.NB_OPERATIONS as double), -999.9) = NVL(cast(DL.NB_OPERATIONS as double), -999.9)
        AND NVL(cast(DW.NB_SERVICES_DISTINCTS as double), -999.9) = NVL(cast(DL.NB_SERVICES_DISTINCTS as double), -999.9)
--         AND NVL(cast(DW.SOLDE_FIN_JOURNEE as double), -999.9) = NVL(cast(DL.SOLDE_FIN_JOURNEE as double), -999.9)
        AND NVL(cast(DW.ARPU_OM as double), -999.9) = NVL(cast(DL.ARPU_OM as double), -999.9)
        AND NVL(cast(DW.NB_CI as double), -999.9) = NVL(cast(DL.NB_CI as double), -999.9)
        AND NVL(cast(DW.MONTANT_CI as double), -999.9) = NVL(cast(DL.MONTANT_CI as double), -999.9)
        AND NVL(cast(DW.NB_CO as double), -999.9) = NVL(cast(DL.NB_CO as double), -999.9)
        AND NVL(cast(DW.MONTANT_CO as double), -999.9) = NVL(cast(DL.MONTANT_CO as double), -999.9)
        AND NVL(cast(DW.FRAIS_CO as double), -999.9) = NVL(cast(DL.FRAIS_CO as double), -999.9)
        AND NVL(cast(DW.NB_BILL_PAY as double), -999.9) = NVL(cast(DL.NB_BILL_PAY as double), -999.9)
        AND NVL(cast(DW.MONTANT_BILL_PAY as double), -999.9) = NVL(cast(DL.MONTANT_BILL_PAY as double), -999.9)
        AND NVL(cast(DW.FRAIS_BILL_PAY as double), -999.9) = NVL(cast(DL.FRAIS_BILL_PAY as double), -999.9)
        AND NVL(cast(DW.NB_MERCHPAY as double), -999.9) = NVL(cast(DL.NB_MERCHPAY as double), -999.9)
        AND NVL(cast(DW.MONTANT_MERCHPAY as double), -999.9) = NVL(cast(DL.MONTANT_MERCHPAY as double), -999.9)
        AND NVL(cast(DW.FRAIS_MERCHPAY as double), -999.9) = NVL(cast(DL.FRAIS_MERCHPAY as double), -999.9)
        AND NVL(cast(DW.MONTANT_P2P_RECU as double), -999.9) = NVL(cast(DL.MONTANT_P2P_RECU as double), -999.9)
        AND NVL(cast(DW.FRAIS_P2P_ORANGE as double), -999.9) = NVL(cast(DL.FRAIS_P2P_ORANGE as double), -999.9)
        AND NVL(cast(DW.NB_P2P_RECU as double), -999.9) = NVL(cast(DL.NB_P2P_RECU as double), -999.9)
        AND NVL(cast(DW.MONTANT_P2P_ORANGE as double), -999.9) = NVL(cast(DL.MONTANT_P2P_ORANGE as double), -999.9)
        AND NVL(cast(DW.MONTANT_TNO as double), -999.9) = NVL(cast(DL.MONTANT_TNO as double), -999.9)
        AND NVL(cast(DW.FRAIS_TNO as double), -999.9) = NVL(cast(DL.FRAIS_TNO as double), -999.9)
        AND NVL(cast(DW.NB_TOP_UP as double), -999.9) = NVL(cast(DL.NB_TOP_UP as double), -999.9)
        AND NVL(cast(DW.MONTANT_TOP_UP as double), -999.9) = NVL(cast(DL.MONTANT_TOP_UP as double), -999.9)
        AND NVL(cast(DW.NB_AUTRES as double), -999.9) = NVL(cast(DL.NB_AUTRES as double), -999.9)
        AND NVL(cast(DW.MONTANT_AUTRES as double), -999.9) = NVL(cast(DL.MONTANT_AUTRES as double), -999.9)
        AND NVL(cast(DW.NB_BUNDLES_DATA as double), -999.9) = NVL(cast(DL.NB_BUNDLES_DATA as double), -999.9)
        AND NVL(cast(DW.MONTANT_BDLE_DATA as double), -999.9) = NVL(cast(DL.MONTANT_BDLE_DATA as double), -999.9)
        AND NVL(cast(DW.NB_BUNDLE_VOIX as double), -999.9) = NVL(cast(DL.NB_BUNDLE_VOIX as double), -999.9)
        AND NVL(cast(DW.MONTANT_BDLE_VOIX as double), -999.9) = NVL(cast(DL.MONTANT_BDLE_VOIX as double), -999.9)
    )
    WHERE
        TO_DATE(DW.PERIOD) = '2020-02-17'
        AND DL.PERIOD IS NULL
) T
