CREATE TABLE CDR.SPARK_IT_MY_OM_ID
(
    PHONE_TANGO VARCHAR(50),
    CGU_ACCEPTED VARCHAR(5),
    CGU_ACCEPTED_AT VARCHAR(50),
    NOM VARCHAR(200),
    PRENOM VARCHAR(200),
    NAISSANCE VARCHAR(50),
    PIECE VARCHAR(200),
    DATEEXPIRE VARCHAR(50),
    GENRE VARCHAR(50),
    STATUT VARCHAR(5),
    AJOUR VARCHAR(5),
    UPDATEDTANGO VARCHAR(5),
    UPDATEDTANGOAT VARCHAR(50),
    UTANGORESPONSECODE VARCHAR(5),
    SCAN_PIECE_AVANT VARCHAR(400),
    SCAN_PIECE_ARRIERE VARCHAR(400),
    SCAN_SIGNATURE_CLIENT VARCHAR(400),
    SCAN_PIECE_AVANT_BOVIEW VARCHAR(400),
    SCAN_PIECE_ARRIERE_BOVIEW VARCHAR(400),
    SCAN_SIGNATURE_BOVIEW VARCHAR(400),
    DATE_VALIDATION_BO VARCHAR(300),
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (FILE_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')