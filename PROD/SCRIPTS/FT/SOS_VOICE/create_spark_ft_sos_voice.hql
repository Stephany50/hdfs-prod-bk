CREATE TABLE MON.SPARK_FT_SOS_VOICE
(	
	MSISDN VARCHAR(100),
	PRICE_PLAN_NAME VARCHAR(25),
	MONTANT BIGINT,
	A_REMBOURSER BIGINT,
	COMMISSION BIGINT,
	STATUS VARCHAR(100)
) PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY");