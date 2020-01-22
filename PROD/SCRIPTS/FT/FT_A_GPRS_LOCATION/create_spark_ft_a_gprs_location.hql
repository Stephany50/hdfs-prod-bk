CREATE TABLE AGG.SPARK_FT_A_GPRS_LOCATION
(	
	SERVED_PARTY_OFFER VARCHAR(100), 
	LOCATION_MCC VARCHAR(25), 
	LOCATION_MNC VARCHAR(25), 
	LOCATION_LAC VARCHAR(25), 
	LOCATION_CI VARCHAR(25), 
	SERVICE_TYPE VARCHAR(100), 
	SERVICE_CODE VARCHAR(100), 
	CALL_TYPE VARCHAR(50), 
	SERVED_PARTY_TYPE VARCHAR(100), 
	MAIN_COST BIGINT, 
	PROMO_COST BIGINT, 
	TOTAL_COST BIGINT, 
	BYTES_SENT BIGINT, 
	BYTES_RECEIVED BIGINT, 
	SESSION_DURATION BIGINT, 
	TOTAL_HITS BIGINT, 
	TOTAL_UNIT BIGINT, 
	BUNDLE_BYTES_USED_VOLUME BIGINT, 
	MAIN_REMAINING_CREDIT BIGINT, 
	PROMO_REMAINING_CREDIT BIGINT, 
	BUNDLE_BYTES_REMAINING_VOLUME BIGINT, 
	BUNDLE_MMS_REMAINING_VOLUME BIGINT, 
	OPERATOR_CODE VARCHAR(25), 
	INSERT_DATE TIMESTAMP, 
	TOTAL_COUNT BIGINT, 
	RATED_COUT BIGINT, 
	MSISDN_COUNT BIGINT, 
	ROAMING_INDICATOR BIGINT
	
) COMMENT 'FT_A_GPRS_LOCATION'
PARTITIONED BY (SESSION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');