SELECT  
DATE_FORMAT(EVENT_DATE, 'dd/MM/yyyy'),
MSISDN,
FORMULE,
CONSO,
SMS_COUNT,
TEL_COUNT,
TEL_DURATION,
BILLED_SMS_COUNT,
BILLED_TEL_COUNT,
BILLED_TEL_DURATION,
CONSO_SMS,
CONSO_TEL,
PROMOTIONAL_CALL_COST,
MAIN_CALL_COST
FROM MON.SPARK_FT_CONSO_MSISDN_DAY
WHERE EVENT_DATE =  '###SLICE_VALUE###'