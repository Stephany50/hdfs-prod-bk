SELECT MSISDN,
TO_DATE(REGISTERED_ON) REGISTRATION_DATE
FROM CDR.SPARK_IT_OM_SUBSCRIBERS
WHERE TO_DATE(REGISTERED_ON)=MODIFICATION_DATE
AND TO_DATE(REGISTERED_ON) BETWEEN DATE_SUB('###SLICE_VALUE###',6) AND '###SLICE_VALUE###' 