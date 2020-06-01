insert into tmp.ft_global_activity_daily
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,source_table,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily where
(transaction_date='2020-05-14' and source_table='FT_OVERDRAFT') or
(transaction_date='2020-05-16' and source_table='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-11' and source_table='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-05-08' and source_table='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-15' and source_table='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-05-17' and source_table='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-09' and source_table='FT_REFILL') or
(transaction_date='2020-05-12' and source_table='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-16' and source_table='FT_REFILL') or
(transaction_date='2020-05-17' and source_table='FT_REFILL') or
(transaction_date='2020-05-09' and source_table='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-14' and source_table='FT_A_DATA_TRANSFER') or
(transaction_date='2020-05-08' and source_table='FT_REFILL')
group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,source_table,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE






insert into global_activity_mkt
select TRANSACTION_DATE,DESTINATION_CODE,PROFILE_CODE,SUB_ACCOUNT,MEASUREMENT_UNIT,SOURCE_TABLE,OPERATOR_CODE,TOTAL_AMOUNT,RATED_AMOUNT,INSERT_DATE,REGION_ID  from agg.spark_ft_global_activity_daily_mkt where
(transaction_date='2020-05-04' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-06' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-06' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-07' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-07' and source_table='FT_CONSO_MSISDN_DAY') or		
(transaction_date='2020-05-07' and source_table='FT_A_SUBSCRIPTION') or		
(transaction_date='2020-05-08' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-08' and source_table='FT_USERS_DATA_DAY') or		
(transaction_date='2020-05-08' and source_table='FT_USERS_DAY') or		
(transaction_date='2020-05-09' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-10' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-10' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-11' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-12' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-12' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-13' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-13' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-14' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-15' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-15' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-16' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-16' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-17' and source_table='FT_GSM_TRAFFIC_REVENUE_DAILY') or		
(transaction_date='2020-05-17' and source_table='FT_A_SUBSCRIBER_SUMMARY') or		
(transaction_date='2020-05-17' and source_table='FT_A_GPRS_LOCATION') or		
(transaction_date='2020-05-17' and source_table='FT_A_GPRS_ACTIVITY') or		
(transaction_date='2020-05-17' and source_table='FT_USERS_DATA_DAY') or		
(transaction_date='2020-05-17' and source_table='FT_A_SUBSCRIPTION') or		
(transaction_date='2020-05-17' and source_table='FT_GSM_LOCATION_REVENUE_DAILY') or		
(transaction_date='2020-05-17' and source_table='FT_ACCOUNT_ACTIVITY') or		
(transaction_date='2020-05-17' and source_table='FT_USERS_DAY') or		
(transaction_date='2020-05-17' and source_table='FT_USERS_REGION_LOCATION') or		
(transaction_date='2020-05-17' and source_table='FT_SUBSCRIPTION_SITE_DAY') or		
(transaction_date='2020-05-17' and source_table='FT_CONSO_MSISDN_DAY') or		
(transaction_date='2020-05-17' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY')
