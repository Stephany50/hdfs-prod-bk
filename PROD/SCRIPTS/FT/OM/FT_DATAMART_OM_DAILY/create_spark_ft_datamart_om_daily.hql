CREATE TABLE MON.SPARK_FT_DATAMART_OM_DAILY
(
    MSISDN VARCHAR(100),
    USER_ID VARCHAR(100),
    NB_OPERATIONS int,
    NB_SERVICES_DISTINCTS int,
    SOLDE_FIN_JOURNEE decimal(17,2),
    ARPU_OM bigint,
    NB_CI int,
    MONTANT_CI int,
    NB_CO int,
    MONTANT_CO int,
    FRAIS_CO bigint,
    NB_BILL_PAY int,
    MONTANT_BILL_PAY int,
    FRAIS_BILL_PAY bigint,
    NB_MERCHPAY int,
    MONTANT_MERCHPAY int,
    FRAIS_MERCHPAY bigint,
    MONTANT_P2P_RECU int,
    NB_P2P_RECU int,
    MONTANT_P2P_ORANGE int,
    FRAIS_P2P_ORANGE bigint,
    MONTANT_TNO int,
    FRAIS_TNO bigint,
    NB_TOP_UP int,
    MONTANT_TOP_UP int,
    NB_AUTRES int,
    MONTANT_AUTRES int,
    NB_BUNDLES_DATA int,
    MONTANT_BDLE_DATA int,
    NB_BUNDLE_VOIX int,
    MONTANT_BDLE_VOIX int,
    INSERT_DATE timestamp,
    category_code varchar(100),
    domain_code varchar(100),
    grade_name varchar(100)

)
PARTITIONED BY(PERIOD DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")




