insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily where
(transaction_date='2020-10-19' and source_data='FT_A_GPRS_ACTIVITY_POST')  or
(transaction_date='2020-10-19' and source_data='FT_GSM_TRAFFIC_REVENUE_POST')  or
(transaction_date='2020-10-20' and source_data='FT_A_GPRS_ACTIVITY_POST')  or
(transaction_date='2020-10-20' and source_data='FT_GSM_TRAFFIC_REVENUE_POST')  or
(transaction_date='2020-10-21' and source_data='FT_A_GPRS_ACTIVITY_POST')  or
(transaction_date='2020-10-21' and source_data='FT_GSM_TRAFFIC_REVENUE_POST')  or
(transaction_date='2020-10-26' and source_data='FT_OVERDRAFT')  or
(transaction_date='2020-10-29' and source_data='FT_A_DATA_TRANSFER')  or
(transaction_date='2020-10-29' and source_data='FT_A_GPRS_ACTIVITY')  or
(transaction_date='2020-10-29' and source_data='FT_A_GPRS_ACTIVITY_POST')  or
(transaction_date='2020-10-29' and source_data='FT_A_SUBSCRIPTION')  or
(transaction_date='2020-10-29' and source_data='FT_CONTRACT_SNAPSHOT')  or
(transaction_date='2020-10-29' and source_data='FT_CREDIT_TRANSFER')  or
(transaction_date='2020-10-29' and source_data='FT_EMERGENCY_DATA')  or
(transaction_date='2020-10-29' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY')  or
(transaction_date='2020-10-29' and source_data='FT_GSM_TRAFFIC_REVENUE_POST')  or
(transaction_date='2020-10-29' and source_data='FT_OVERDRAFT')  or
(transaction_date='2020-10-29' and source_data='FT_REFILL')  or
(transaction_date='2020-10-29' and source_data='FT_SUBS_RETAIL_ZEBRA')  or
(transaction_date='2020-10-29' and source_data='IT_ZTE_ADJUSTMENT')
group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE






insert into tmp.SQ_TMP_GLOBAL_ACTIVITY_DAILY
select TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,SUM(RATED_COUNT) RATED_COUNT,SUM(RATED_VOLUME) RATED_VOLUME,SUM(TAXED_AMOUNT) TAXED_AMOUNT,SUM(UNTAXED_AMOUNT) UNTAXED_AMOUNT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE from agg.spark_ft_global_activity_daily
where
(transaction_date='2020-07-25')
group by TRANSACTION_DATE,COMMERCIAL_OFFER_CODE,TRANSACTION_TYPE,SUB_ACCOUNT,TRANSACTION_SIGN,SOURCE_PLATFORM,SOURCE_DATA,SERVED_SERVICE,SERVICE_CODE,DESTINATION,OTHER_PARTY_ZONE,MEASUREMENT_UNIT,INSERT_DATE,TRAFFIC_MEAN,OPERATOR_CODE


select datecode,a.source_data from
    (
    select datecode, source_data from (select * from  dim.dt_dates where datecode between '10/09/20' and '15/10/20'
 )a
 cross join (
    select distinct source_data from mon.ft_global_activity_daily where transaction_date='28/06/20'
    ) b) a left join (select transaction_date, source_data from mon.ft_global_activity_daily where transaction_date>='10/09/20'
) b on a.datecode=b.transaction_date and a.source_data=b.source_data where b.source_data is null  ORDER BY 1,2;



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






SELECT refill_date, SUM(REFILL_AMOUNT)  C2S_VAS
       FROM MON.spark_ft_refill
       where refill_date >= '2020-09-01'
       AND REFILL_AMOUNT>0
       AND TERMINATION_IND='200'
       AND REFILL_MEAN ='C2S'
       AND REFILL_TYPE  ='PVAS'
       AND SENDER_CATEGORY IN ('NPOS','PPOS', 'INHSM')
       group by refill_date order by refill_date;


SELECT TRANSFER_DATE, SUM((CASE WHEN NVL(RECEIVER_CREDIT_AMOUNT, 0) = 0 THEN 0
             ELSE (NVL(RECEIVER_CREDIT_AMOUNT,0))/100
    END))  C2S_VAS
       FROM (

                 SELECT
                     TRANSFER_ID
                      ,TRANSFER_DATE_TIME
                      ,RECEIVER_MSISDN
                      ,SENDER_MSISDN
                      ,CHANNEL_TYPE
                      ,TRANSACTION_TYPE
                      ,RECEIVER_CREDIT_AMOUNT
                      ,TRANSFER_STATUS
                      ,INSERT_DATE
                      ,ORIGINAL_FILE_NAME
                      ,SENDER_CATEGORY
                      ,RECEIVER_CATEGORY
                      ,SEND_PRE_BAL
                      ,SEND_POST_BAL
                      ,RCVR_PRE_BAL
                      ,RCVR_POST_BAL
                      ,SERVICE_CLASS_CODE
                      ,TRANSFER_DATE
                      ,ID
                      ,IF(BUNDLE_DET<>'' AND SPLIT(BUNDLE_DET, '&')[1] IN ('1','4'),CAST(SPLIT(BUNDLE_DET, '&')[2] AS DOUBLE),0) MAIN_BUNDLE_VAL
                      ,IF(BUNDLE_DET<>'' AND SPLIT(BUNDLE_DET, '&')[1] IN ('2','3','10','15','16','30','72'),CAST(SPLIT(BUNDLE_DET, '&')[2] AS DOUBLE),0) BONUS_BUNDLE_VAL
                      , OTHER_COMMISION
                 FROM (
                          SELECT A.*, ROW_NUMBER() OVER(ORDER BY TRANSFER_DATE) ID
                          FROM CDR.SPARK_IT_ZEBRA_TRANSAC A
                          WHERE A.TRANSFER_DATE >= '2020-09-01'
                      ) A
                          LATERAL VIEW EXPLODE(SPLIT(NVL(BONUS_ACCOUNT_DETAILS, ''), '[|]')) TMP AS BUNDLE_DET
       )ZEBRA
WHERE TRANSFER_DATE>= '2020-09-01'
AND TRANSFER_STATUS ='200'
AND CHANNEL_TYPE='C2S'
AND TRANSACTION_TYPE ='PVAS'
AND SENDER_CATEGORY IN ('NPOS','PPOS', 'INHSM')
GROUP BY TRANSFER_DATE ORDER BY TRANSFER_DATE