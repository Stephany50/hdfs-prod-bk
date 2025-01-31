CREATE TABLE MON.SPARK_FT_MSC_CMR_BAD_CALL(
    MOIS VARCHAR(6),
	TRANSACTION_TIME VARCHAR(6),
	TRANSACTION_DIRECTION VARCHAR(40),
	TRANSACTION_TYPE VARCHAR(40),
	SERVED_MSISDN VARCHAR(40 ),
	MSISDN_OPERATOR VARCHAR(4000),
	SERVED_PARTY_LOCATION VARCHAR(25),
	OTHER_PARTY VARCHAR(40),
	OTHER_OPERATOR VARCHAR(4000),
	PARTNER_GT VARCHAR(40),
	TRANSACTION_DURATION int,
	TRUNCK_IN VARCHAR(50),
	FAISCEAU_IN VARCHAR(5),
	TRUNCK_OUT VARCHAR(50),
	FAISCEAU_OUT VARCHAR(5),
	MSC_ADRESS VARCHAR(25),
	TRANSACTION_SERVICE_CODE VARCHAR(6),
	RECORD_TYPE VARCHAR(6),
	OLD_CALLING_NUMBER VARCHAR(40) ,
	OLD_CALLING_OPERATOR VARCHAR(4000),
	OLD_CALLED_NUMBER VARCHAR(40 ),
	OLD_CALLED_OPERATOR VARCHAR(4000),
	ROAMING_NUMBER VARCHAR(40),
	OPERATOR_PREFIX VARCHAR(75),
    INSERT_DATE TIMESTAMP
)
COMMENT ' SPARK_FT_MSC_CMR_BAD_CALL '
 PARTITIONED BY (TRANSACTION_DATE DATE)
 STORED AS PARQUET
 TBLPROPERTIES ("parquet.compress"="SNAPPY")
