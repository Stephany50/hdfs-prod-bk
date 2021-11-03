INSERT INTO MON.SPARK_FT_TMP_ACQ_SNAP_MONTH
SELECT 
    ACCESS_KEY MSISDN,
	
    NULL FIRST_DAY_REFILL,
    NULL FIRST_DATE_REFILL_AMOUNT,

	NULL REFILL_AMOUNT,
    NULL conso_amount,
    NULL depot_amount,
    NULL souscriptiondata_amount,
    NULL transaction_AMOUNT,

    NULL FIRST_MONTH_CONSO,
    NULL FIRST_MONTH_DEPOT,
	NULL FIRST_MONTH_REFILL,
	NULL FIRST_MONTH_SOUSCRIPTIONDATA,
	NULL FIRST_MONTH_TRANSACTION,
	
	NULL SECOND_MONTH_CONSO,
	NULL SECOND_MONTH_DEPOT,
	NULL SECOND_MONTH_REFILL,
	NULL SECOND_MONTH_SOUSCRIPTIONDATA,
	NULL SECOND_MONTH_TRANSACTION,
	
	NULL THIRD_MONTH_CONSO,
	NULL THIRD_MONTH_DEPOT,
	NULL THIRD_MONTH_REFILL,
	NULL THIRD_MONTH_SOUSCRIPTIONDATA,
	NULL THIRD_MONTH_TRANSACTION,
		
    CURRENT_TIMESTAMP()   INSERT_DATE,
    SUBSTR(ACTIVATION_DATE, 1, 7)   EVENT_MONTH,
    ACTIVATION_DATE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1) AND ACTIVATION_DATE = '###SLICE_VALUE###'