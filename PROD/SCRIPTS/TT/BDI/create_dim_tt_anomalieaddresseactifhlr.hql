CREATE  TABLE DIM.DT_ANOMALIEADDRESSEACTIFHLR
NUMERO_PIECE VARCHAR(255),
VALID_FROM TIMESTAMP,
VALID_TO   TIMESTAMP,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (FILE_DATE DATE)
CLUSTERED BY(NUMERO_PIECE) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')