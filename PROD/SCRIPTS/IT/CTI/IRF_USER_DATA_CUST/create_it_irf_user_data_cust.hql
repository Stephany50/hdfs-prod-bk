CREATE TABLE CTI.IT_IRF_USER_DATA_CUST (
EVENT_TIME TIMESTAMP,
TS VARCHAR(15),
CONNID VARCHAR(30),
ANI VARCHAR(15),
DNIS VARCHAR(15),
LAST_VQ VARCHAR(50),
TECHNICAL_RESULT VARCHAR(50),
TECHNICAL_RESULT_CODE VARCHAR(50),
RESULT_REASON VARCHAR(50),
RESULT_REASON_CODE VARCHAR(50),
DUREE_CONVERSATION INT,
PLACE_KEY INT,
UD_SITE_CHOISI VARCHAR(15),
INTERACTION_TYPE VARCHAR(15),
INTERACTION_TYPE_CODE VARCHAR(15),
PLACE VARCHAR(15),
RESOURCE_TYPE VARCHAR(15),
RESOURCE_NAME VARCHAR(15),
NOM VARCHAR(255),
DUREE_FILE INT,
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_DATE DATE,
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE,FILE_DATE DATE)
CLUSTERED BY(LAST_VQ,UD_SITE_CHOISI) INTO 4 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;