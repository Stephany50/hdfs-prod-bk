

CREATE TABLE  CDR.SPARK_IT_RIGHTQ_TRANSACTIONS

(

ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
Agence VARCHAR(200),
Ticket VARCHAR(10) ,
Service VARCHAR(100),
Client VARCHAR(100),
Mobile VARCHAR(50),
Arrivee TIMESTAMP,
temps_Attente INT,
temps_Service INT,
Note VARCHAR(100),
Commentaire VARCHAR(100),
Status VARCHAR(10),
Utilisateur VARCHAR(100),
Updated_tickets VARCHAR(100),
email VARCHAR(100),
INSERT_DATE TIMESTAMP

)

PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(WALLET_NUMBER) INTO 64 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')