insert into MON.SPARK_FT_GROSSADD_DAY
SELECT DISTINCT 
    SERVED_PARTY_MSISDN msisdn, 
    last_day(transaction_date) last_day_month, 
    current_timestamp insert_date,
    transaction_date
FROM MON.SPARK_FT_SUBSCRIPTION
WHERE TRANSACTION_DATE  = '###SLICE_VALUE###' 
AND SUBSCRIPTION_SERVICE LIKE '%PPS%'