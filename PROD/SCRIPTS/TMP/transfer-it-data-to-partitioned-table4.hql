insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily where
(transaction_date='2020-05-25' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or 
(transaction_date='2020-05-25' and source_data='FT_A_GPRS_ACTIVITY_POST') or 
(transaction_date='2020-05-28' and source_data='FT_REFILL') or 
(transaction_date='2020-05-31' and source_data='FT_A_GPRS_ACTIVITY')
group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE
(transaction_date='2020-$2-$1' and source_data='$4') or

(\d+)\/(\d+)\/(\d+)\s+(\w+)



insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY_MKT
select TRANSACTION_DATE,DESTINATION_CODE,PROFILE_CODE,SUB_A     CCOUNT,MEASUREMENT_UNIT,SOURCE_TABLE,OPERATOR_CODE,TOTAL_AMOUNT,RATED_AMOUNT,INSERT_DATE,REGION_ID  from agg.spark_ft_global_activity_daily_mkt where
(transaction_date='2020-05-27' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date='2020-05-27' and source_table='FT_ACCOUNT_ACTIVITY') or 
(transaction_date='2020-05-27' and source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date='2020-05-28' and source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date='2020-05-28' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date='2020-05-28' and source_table='FT_ACCOUNT_ACTIVITY') or 
(transaction_date='2020-05-30' and source_table='FT_USERS_DATA_DAY') or 
(transaction_date='2020-05-30' and source_table='FT_USERS_DAY') or 
(transaction_date='2020-05-31' and source_table='FT_USERS_DATA_DAY') or 
(transaction_date='2020-05-31' and source_table='FT_A_SUBSCRIPTION') or 
(transaction_date='2020-05-31' and source_table='FT_GSM_LOCATION_REVENUE_DAILY') or 
(transaction_date='2020-05-31' and source_table='FT_GSM_TRAFFIC_REVENUE_DAILY') or 
(transaction_date='2020-05-31' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or 
(transaction_date='2020-05-31' and source_table='FT_USERS_REGION_LOCATION') or 
(transaction_date='2020-05-31' and source_table='FT_A_SUBSCRIBER_SUMMARY') or 
(transaction_date='2020-05-31' and source_table='FT_USERS_DAY') or 
(transaction_date='2020-05-31' and source_table='FT_SUBSCRIPTION_SITE_DAY') or 
(transaction_date='2020-05-31' and source_table='FT_ACCOUNT_ACTIVITY') or 
(transaction_date='2020-05-31' and source_table='FT_CONSO_MSISDN_DAY') or 
(transaction_date='2020-05-31' and source_table='FT_A_GPRS_ACTIVITY') or 
(transaction_date='2020-05-31' and source_table='FT_A_GPRS_LOCATION')