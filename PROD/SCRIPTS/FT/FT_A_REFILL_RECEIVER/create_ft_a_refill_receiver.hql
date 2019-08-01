
	CREATE TABLE AGG.FT_A_REFILL_RECEIVER
(
	RECEIVER_MSISDN VARCHAR(20), 
	REFILL_TYPE VARCHAR(25), 
	REFILL_MEAN VARCHAR(25), 
	RECEIVER_PROFILE VARCHAR(25), 
	REFILL_AMOUNT DOUBLE, 
	REFILL_BONUS DOUBLE, 
	REFILL_DISCOUNT DOUBLE, 
	REFILL_COUNT DOUBLE, 
	LAST_UPDATE timestamp , 
	OPERATOR_CODE VARCHAR(25)
) COMMENT 'FT_A REFILL_RECEIVER'
PARTITIONED BY (REFILL_MONTH HOUR)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")