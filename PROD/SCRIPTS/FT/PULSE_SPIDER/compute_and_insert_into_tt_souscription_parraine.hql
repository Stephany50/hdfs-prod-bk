insert into TMP.TT_SOUSCRIPTION_PARRAINE
SELECT MSISDN_PARRAIN, REGISTER_TIME, MSISDN_PARRAINE, C.*, CASE WHEN TRANSACTION_DATETIME BETWEEN REGISTER_TIME AND FROM_UNIXTIME(UNIX_TIMESTAMP(REGISTER_TIME,'yyyy-MM-dd HH:mm:ss')+48*3600) THEN 'OK' else 'NOK' END STATUT,
    current_timestamp() insert_date, c.TRANSACTION_DATE SDATE
FROM CDR.SPARK_IT_PULSE_SPIDER_PARRAIN T
LEFT JOIN (
SELECT SERVED_PARTY_MSISDN, TRANSACTION_DATE, TRANSACTION_TIME, SUBSCRIPTION_SERVICE_DETAILS, RATED_AMOUNT,
    FROM_UNIXTIME(UNIX_TIMESTAMP(concat(TRANSACTION_DATE,' ', TRANSACTION_TIME),'yyyy-MM-dd HHmmss')+3600) TRANSACTION_DATETIME, BEGIN_DATE, END_DATE,
    ROW_NUMBER() OVER (PARTITION BY SERVED_PARTY_MSISDN order by cast(TRANSACTION_TIME as int)) RN
FROM MON.SPARK_FT_SUBSCRIPTION A -- tmp.tt_pulse_subs_test a--
JOIN CDR.SPARK_IT_PULSE_SPIDER_FORFAIT B ON (IPP_NAME = SUBSCRIPTION_SERVICE_DETAILS or IPP_CODE = SUBSCRIPTION_SERVICE_DETAILS)
WHERE TRANSACTION_DATE='###SLICE_VALUE###'
    AND B.ORIGINAL_FILE_DATE = ( SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.SPARK_IT_PULSE_SPIDER_FORFAIT )
    AND TRANSACTION_DATE BETWEEN BEGIN_DATE AND nvl(END_DATE, date_sub(current_date,1))
) C ON SERVED_PARTY_MSISDN = MSISDN_PARRAINE --and RN=1
WHERE T.ORIGINAL_FILE_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',2) and '###SLICE_VALUE###'
    and REGISTER_DATE is not null
ORDER BY MSISDN_PARRAIN, SERVED_PARTY_MSISDN