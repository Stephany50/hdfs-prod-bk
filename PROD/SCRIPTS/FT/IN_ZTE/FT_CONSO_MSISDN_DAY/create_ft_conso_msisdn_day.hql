CREATE TABLE MON.FT_CONSO_MSISDN_DAY 
   (	
	EVENT_DATE DATE, 
	MSISDN VARCHAR(25), 
	FORMULE VARCHAR(70), 
	CONSO  BIGINT, 
	SMS_COUNT  BIGINT, 
	TEL_COUNT  BIGINT, 
	TEL_DURATION  BIGINT, 
	BILLED_SMS_COUNT  BIGINT, 
	BILLED_TEL_COUNT  BIGINT, 
	BILLED_TEL_DURATION  BIGINT, 
	CONSO_SMS  BIGINT, 
	CONSO_TEL  BIGINT, 
	PROMOTIONAL_CALL_COST  BIGINT, 
	MAIN_CALL_COST  BIGINT, 
	SRC_TABLE VARCHAR(30), 
	INSERT_DATE DATE, 
	OTHERS_VAS_TOTAL_COUNT  BIGINT, 
	OTHERS_VAS_DURATION  BIGINT, 
	OTHERS_VAS_MAIN_COST  BIGINT, 
	OTHERS_VAS_PROMO_COST  BIGINT, 
	NATIONAL_TOTAL_COUNT  BIGINT, 
	NATIONAL_SMS_COUNT  BIGINT, 
	NATIONAL_DURATION  BIGINT, 
	NATIONAL_MAIN_COST  BIGINT, 
	NATIONAL_PROMO_COST  BIGINT, 
	NATIONAL_SMS_MAIN_COST  BIGINT, 
	NATIONAL_SMS_PROMO_COST  BIGINT, 
	MTN_TOTAL_COUNT  BIGINT, 
	MTN_SMS_COUNT  BIGINT, 
	MTN_DURATION  BIGINT, 
	MTN_TOTAL_CONSO  BIGINT, 
	MTN_SMS_CONSO  BIGINT, 
	CAMTEL_TOTAL_COUNT  BIGINT, 
	CAMTEL_SMS_COUNT  BIGINT, 
	CAMTEL_DURATION  BIGINT, 
	CAMTEL_TOTAL_CONSO  BIGINT, 
	CAMTEL_SMS_CONSO  BIGINT, 
	INTERNATIONAL_TOTAL_COUNT  BIGINT, 
	INTERNATIONAL_SMS_COUNT  BIGINT, 
	INTERNATIONAL_DURATION  BIGINT, 
	INTERNATIONAL_TOTAL_CONSO  BIGINT, 
	INTERNATIONAL_SMS_CONSO  BIGINT, 
	ONNET_SMS_COUNT  BIGINT, 
	ONNET_DURATION  BIGINT, 
	ONNET_TOTAL_CONSO  BIGINT, 
	ONNET_MAIN_CONSO  BIGINT, 
	ONNET_MAIN_TEL_CONSO  BIGINT, 
	ONNET_PROMO_TEL_CONSO  BIGINT, 
	ONNET_SMS_CONSO  BIGINT, 
	MTN_MAIN_CONSO  BIGINT, 
	CAMTEL_MAIN_CONSO  BIGINT, 
	SET_TOTAL_COUNT  BIGINT, 
	SET_SMS_COUNT  BIGINT, 
	SET_DURATION  BIGINT, 
	SET_TOTAL_CONSO  BIGINT, 
	SET_SMS_CONSO  BIGINT, 
	SET_MAIN_CONSO  BIGINT, 
	INTERNATIONAL_MAIN_CONSO  BIGINT, 
	ROAM_TOTAL_COUNT  BIGINT, 
	ROAM_SMS_COUNT  BIGINT, 
	ROAM_DURATION  BIGINT, 
	ROAM_TOTAL_CONSO  BIGINT, 
	ROAM_MAIN_CONSO  BIGINT, 
	ROAM_SMS_CONSO  BIGINT, 
	INROAM_TOTAL_COUNT  BIGINT, 
	INROAM_SMS_COUNT  BIGINT, 
	INROAM_DURATION  BIGINT, 
	INROAM_TOTAL_CONSO  BIGINT, 
	INROAM_MAIN_CONSO  BIGINT, 
	INROAM_SMS_CONSO  BIGINT, 
	OPERATOR_CODE VARCHAR(50), 
	NEXTTEL_TOTAL_COUNT  BIGINT, 
	NEXTTEL_SMS_COUNT  BIGINT, 
	NEXTTEL_DURATION  BIGINT, 
	NEXTTEL_TOTAL_CONSO  BIGINT, 
	NEXTTEL_SMS_CONSO  BIGINT, 
	NEXTTEL_MAIN_CONSO  BIGINT, 
	BUNDLE_SMS_COUNT  BIGINT, 
	BUNDLE_TEL_DURATION  BIGINT, 
	SET_MAIN_TEL_CONSO  BIGINT, 
	MTN_MAIN_TEL_CONSO  BIGINT, 
	NEXTTEL_MAIN_TEL_CONSO  BIGINT, 
	CAMTEL_MAIN_TEL_CONSO  BIGINT, 
	INTERNATIONAL_MAIN_TEL_CONSO  BIGINT, 
	SET_PROMO_TEL_CONSO  BIGINT, 
	MTN_PROMO_TEL_CONSO  BIGINT, 
	NEXTTEL_PROMO_TEL_CONSO  BIGINT, 
	CAMTEL_PROMO_TEL_CONSO  BIGINT, 
	INTERNATIONAL_PROMO_TEL_CONSO  BIGINT, 
	ROAM_MAIN_TEL_CONSO  BIGINT, 
	INROAM_MAIN_TEL_CONSO  BIGINT, 
	ROAM_PROMO_TEL_CONSO  BIGINT, 
	INROAM_PROMO_TEL_CONSO  BIGINT, 
	ONNET_BILLED_TEL_DURATION  BIGINT, 
	SET_BILLED_TEL_DURATION  BIGINT, 
	MTN_BILLED_TEL_DURATION  BIGINT, 
	NEXTTEL_BILLED_TEL_DURATION  BIGINT, 
	CAMTEL_BILLED_TEL_DURATION  BIGINT, 
	INTERNATIONAL_BIL_TEL_DURATION  BIGINT, 
	ROAM_BILLED_TEL_DURATION  BIGINT, 
	INROAM_BILLED_TEL_DURATION  BIGINT, 
	ONNET_BILLED_TEL_COUNT  BIGINT, 
	SET_BILLED_TEL_COUNT  BIGINT, 
	MTN_BILLED_TEL_COUNT  BIGINT, 
	NEXTTEL_BILLED_TEL_COUNT  BIGINT, 
	CAMTEL_BILLED_TEL_COUNT  BIGINT, 
	INTERNATIONAL_BILLED_TEL_COUNT  BIGINT, 
	ROAM_BILLED_TEL_COUNT  BIGINT, 
	INROAM_BILLED_TEL_COUNT  BIGINT, 
	ONNET_TEL_COUNT  BIGINT, 
	SET_TEL_COUNT  BIGINT, 
	MTN_TEL_COUNT  BIGINT, 
	NEXTTEL_TEL_COUNT  BIGINT, 
	CAMTEL_TEL_COUNT  BIGINT, 
	INTERNATIONAL_TEL_COUNT  BIGINT, 
	ROAM_TEL_COUNT  BIGINT, 
	INROAM_TEL_COUNT  BIGINT, 
	CONSO_TEL_MAIN  BIGINT, 
	SVA_COUNT  BIGINT, 
	SVA_DURATION  BIGINT, 
	SVA_MAIN_CONSO  BIGINT, 
	SVA_PROMO_CONSO  BIGINT, 
	SVA_TEL_COUNT  BIGINT, 
	SVA_BILLED_DURATION  BIGINT, 
	SVA_BILLED_TEL_CONSO  BIGINT, 
	SVA_SMS_COUNT  BIGINT, 
	SVA_SMS_CONSO  BIGINT
   ) 
COMMENT 'FT_CONSO_MSISDN_DAY'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',orc.compress=ZLIB,orc.stripe.size=67108864);