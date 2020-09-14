

CREATE TABLE  CDR.SPARK_IT_RIGHTQ_REF_AGENTS

(
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
Agence VARCHAR(200),
Nom_agent VARCHAR(200),
Email VARCHAR(200),
Statut VARCHAR(20),
Mobile VARCHAR(50),
Profile VARCHAR(20),
Date_creation TIMESTAMP,
Date_derniere_modif TIMESTAMP,
INSERT_DATE TIMESTAMP

)

PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(WALLET_NUMBER) INTO 64 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')