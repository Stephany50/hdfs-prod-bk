CREATE TABLE AGG.FT_A_EMERGENCY_DATA (
	NBRE_SOUSCRIPTION_UNIQUE BIGINT, 
	MONTANT_LOAN BIGINT, 
	MONTANT_PAYBACK BIGINT, 
	NBRE_SOUSCRIPTION BIGINT, 
	BYTES_OBTAINED BIGINT, 
	TRANSACTION_TYPE VARCHAR(20), 
	CONTACT_CHANNEL VARCHAR(20), 
	BUNDLE VARCHAR(6), 
	OFFER_PROFILE_CODE VARCHAR(100), 
	OPERATOR_CODE VARCHAR(50), 
	INSERT_DATE TIMESTAMP
    ) COMMENT 'AGG.FT_A_EMERGENCY_DATA Table'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
    