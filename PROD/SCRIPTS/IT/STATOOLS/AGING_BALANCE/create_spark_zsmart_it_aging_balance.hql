CREATE TABLE CDR.SPARK_IT_AGING_BALANCE
(
    code_client VARCHAR(200),
    account_number VARCHAR(200),
    account_name VARCHAR(200),
    categ VARCHAR(64),
    payment_term VARCHAR(64),
    statut VARCHAR(64),
    nom VARCHAR(200),
    balance DECIMAL,
    date_derniere_facture DATE,
    au_zero_jrs DECIMAL,
    au_moins_trente_jrs DECIMAL,
    au_moins_soixante_jrs DECIMAL,
    au_moins_quatre_vingt_dix_jrs DECIMAL,
    au_moins_cent_vingt_jrs DECIMAL,
    au_moins_cent_cinquante_jrs DECIMAL,
    au_moins_cent_quatre_vingt_jrs DECIMAL,
    au_moins_trois_cent_soixante_jrs DECIMAL,
    au_moins_sept_cent_vingt_jrs DECIMAL,
    sup_sept_vent_vingt_jrs DECIMAL,
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (AS_OF_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_AGING_BALANCE
(
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    as_of_date DATE,
    code_client VARCHAR(200),
    account_number VARCHAR(200),
    account_name VARCHAR(200),
    categ VARCHAR(64),
    payment_term VARCHAR(64),
    statut VARCHAR(64),
    nom VARCHAR(200),
    balance DECIMAL,
    date_derniere_facture DATE,
    au_zero_jrs DECIMAL,
    au_moins_trente_jrs DECIMAL,
    au_moins_soixante_jrs DECIMAL,
    au_moins_quatre_vingt_dix_jrs DECIMAL,
    au_moins_cent_vingt_jrs DECIMAL,
    au_moins_cent_cinquante_jrs DECIMAL,
    au_moins_cent_quatre_vingt_jrs DECIMAL,
    au_moins_trois_cent_soixante_jrs DECIMAL,
    au_moins_sept_cent_vingt_jrs DECIMAL,
    sup_sept_vent_vingt_jrs DECIMAL
)COMMENT 'CDR TT_AGING_BALANCE external table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
LOCATION '/PROD/TT/ZSMART/AGING_BALANCE'
TBLPROPERTIES ('serialization.null.format'='');