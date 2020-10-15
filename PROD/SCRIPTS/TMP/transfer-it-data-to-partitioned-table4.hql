insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily where
(transaction_date='2020-08-03' and source_data='FT_GSM_TRAFFIC_REVENUE_POST')  or
(transaction_date='2020-08-12' and source_data='FT_A_GPRS_ACTIVITY_POST')  or
(transaction_date='2020-08-16' and source_data='FT_CREDIT_TRANSFER')  or
(transaction_date='2020-08-30' and source_data='FT_A_DATA_TRANSFER')  or
(transaction_date='2020-08-30' and source_data='FT_A_GPRS_ACTIVITY')  or
(transaction_date='2020-08-30' and source_data='FT_A_GPRS_ACTIVITY_POST')  or
(transaction_date='2020-08-30' and source_data='FT_A_SUBSCRIPTION')  or
(transaction_date='2020-08-30' and source_data='FT_CONTRACT_SNAPSHOT')  or
(transaction_date='2020-08-30' and source_data='FT_CREDIT_TRANSFER')  or
(transaction_date='2020-08-30' and source_data='FT_EMERGENCY_DATA')  or
(transaction_date='2020-08-30' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY')  or
(transaction_date='2020-08-30' and source_data='FT_GSM_TRAFFIC_REVENUE_POST')  or
(transaction_date='2020-08-30' and source_data='FT_OVERDRAFT')  or
(transaction_date='2020-08-30' and source_data='IT_ZTE_ADJUSTMENT')

group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE







insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily
where
(transaction_date='2020-07-25')
group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE


select datecode,a.source_data from
    (
    select datecode, source_data from (select * from  dim.dt_dates where datecode between '2020-08-01' and '2020-08-16'
 )a
 cross join (
    select distinct source_data from agg.spark_ft_global_activity_daily where transaction_date='2020-06-28'
    ) b) a left join (select transaction_date, source_data from agg.spark_ft_global_activity_daily where transaction_date>='2020-08-01'
) b on a.datecode=b.transaction_date and a.source_data=b.source_data where b.source_data is null  ODER BY 1,2;



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

INSERT INTO  MON.REVENNU_VOIX_SMS
SELECT
mois,
TRIM(loc_admintrative_region) LOC_ADMINTRATIVE_REGION,
AVG(Souscription_Voix + principal_voix) REVENU_VOIX,
AVG(Souscription_SMS + principal_SMS) REVENU_SMS
FROM(
select mois, sum (c.voix) as Souscription_Voix,   sum (d.MAIN_RATED_TEL_AMOUNT) as principal_voix ,SUM(c.SMS) AS Souscription_SMS,SUM(MAIN_RATED_SMS_AMOUNT) AS principal_sms, TRIM(loc_admintrative_region) loc_admintrative_region
from
(select TO_CHAR(period,'yyyymm') as mois, msisdn,
sum(nvl(bdle_cost*COEFF_ONNET_VOICE/100,0) + nvl(bdle_cost*COEFF_OFFNET_VOICE/100,0) + nvl(bdle_cost*COEFF_INTER_VOICE/100,0) + nvl(bdle_cost*COEFF_ROAMING_VOICE/100,0)) as voix,
sum(nvl(bdle_cost*COEFF_SMS/100,0) + nvl(bdle_cost*COEFF_ROAMING_SMS/100,0)) as SMS
from  mon.FT_CBM_BUNDLE_SUBS_DAILY a, dim.ref_souscription b
where period between '01/01/2019' and LAST_DAY('01/01/2019')
and upper(trim(a.bdle_name))= upper(trim(b.bdle_name))
group by TO_CHAR(period,'yyyymm'), msisdn ) c,
(select distinct event_month, msisdn,MAIN_RATED_TEL_AMOUNT, MAIN_RATED_SMS_AMOUNT, DATA_MAIN_RATED_AMOUNT,TRIM(loc_admintrative_region) loc_admintrative_region
from MON.FT_MARKETING_DATAMART_MONTH
where event_month ='201901') d
where
c.msisdn=d.msisdn and
c.mois=d.event_month
group by TRIM(loc_admintrative_region),mois) T
GROUP BY mois,
TRIM(loc_admintrative_region)