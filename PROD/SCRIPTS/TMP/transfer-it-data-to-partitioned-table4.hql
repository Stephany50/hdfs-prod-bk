insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily where
(transaction_date='2020-07-08' and source_data='FT_A_GPRS_ACTIVITY_POST') or
(transaction_date='2020-07-08' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or
(transaction_date='2020-07-08' and source_data='FT_OVERDRAFT') or
(transaction_date='2020-07-24' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or
(transaction_date='2020-07-26' and source_data='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-07-26' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-07-26' and source_data='FT_OVERDRAFT') or
(transaction_date='2020-07-26' and source_data='FT_REFILL')

group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE

(transaction_date='2020-07-08' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or
(transaction_date='2020-07-08' and source_data='FT_OVERDRAFT') or
(transaction_date='2020-07-24' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or
(transaction_date='2020-07-24' and source_data='FT_REFILL') or
(transaction_date='2020-07-25' and source_data='FT_A_DATA_TRANSFER') or
(transaction_date='2020-07-25' and source_data='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-07-25' and source_data='FT_A_GPRS_ACTIVITY_POST') or
(transaction_date='2020-07-25' and source_data='FT_A_SUBSCRIPTION') or
(transaction_date='2020-07-25' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-07-25' and source_data='FT_REFILL') or
(transaction_date='2020-07-26' and source_data='FT_A_DATA_TRANSFER') or
(transaction_date='2020-07-26' and source_data='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-07-26' and source_data='FT_A_GPRS_ACTIVITY_POST') or
(transaction_date='2020-07-26' and source_data='FT_A_SUBSCRIPTION') or
(transaction_date='2020-07-26' and source_data='FT_CONTRACT_SNAPSHOT') or
(transaction_date='2020-07-26' and source_data='FT_CREDIT_TRANSFER') or
(transaction_date='2020-07-26' and source_data='FT_EMERGENCY_DATA') or
(transaction_date='2020-07-26' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-07-26' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or
(transaction_date='2020-07-26' and source_data='FT_OVERDRAFT') or
(transaction_date='2020-07-26' and source_data='FT_REFILL') or
(transaction_date='2020-07-26' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-07-26' and source_data='IT_ZTE_ADJUSTMENT')  or
(transaction_date='2020-07-08' and source_data='FT_A_GPRS_ACTIVITY_POST')







insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily
where
(transaction_date='2020-07-25')
group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE




(transaction_date='2020-$2-$1' and source_data='$4') or

(\d+)\/(\d+)\/(\d+)\s+(\w+)
(transaction_date='2020-$2-$1' and source_data='$4')  or



insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY_MKT
select TRANSACTION_DATE,DESTINATION_CODE,PROFILE_CODE,SUB_ACCOUNT,MEASUREMENT_UNIT,SOURCE_TABLE,OPERATOR_CODE,TOTAL_AMOUNT,RATED_AMOUNT,INSERT_DATE,REGION_ID  from agg.spark_ft_global_activity_daily_mkt where
 (transaction_date = '2020-07-02'   and  source_table='FT_ACCOUNT_ACTIVITY') or
(transaction_date = '2020-07-02'   and  source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-02'   and  source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-02'   and  source_table='FT_SUBSCRIPTION_SITE_DAY') or 
(transaction_date = '2020-07-03'   and  source_table='FT_ACCOUNT_ACTIVITY') or 
(transaction_date = '2020-07-03'   and  source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-03'   and  source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-03'   and  source_table='FT_SUBSCRIPTION_SITE_DAY') or 
(transaction_date = '2020-07-04'   and  source_table='FT_ACCOUNT_ACTIVITY') or 
(transaction_date = '2020-07-04'   and  source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-04'   and  source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-04'   and  source_table='FT_SUBSCRIPTION_SITE_DAY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_ACCOUNT_ACTIVITY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_SUBSCRIPTION_SITE_DAY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_USERS_DATA_DAY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_USERS_DAY') or 
(transaction_date = '2020-07-05'   and  source_table='FT_USERS_REGION_LOCATION') or 

