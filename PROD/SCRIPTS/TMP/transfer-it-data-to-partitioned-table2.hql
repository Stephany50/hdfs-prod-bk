add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar ;
create temporary function fn_format_msisdn_to_9digits as 'cm.orange.bigdata.udf.GetNnpMsisdn9Digits';
create table  TMP.LOCALISATION_ABONNES2 as
select
      msisdn,
        region,
        location_ci,
        served_party_location,
        duree_sortant,
        duree_entrant,
        nbre_sms_sortant,
        nbre_sms_entrant,
        traffic
from (

    select
        msisdn,
        region,
        location_ci,
        served_party_location,
        duree_sortant,
        duree_entrant,
        nbre_sms_sortant,
        nbre_sms_entrant,
        traffic,
        ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY traffic DESC) AS Rang
    from (
    )t
)t
where rang=1
--group by msisdn

(transaction_date='2020-05-17' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-17' and source_data='FT_REFILL') or
(transaction_date='2020-05-18' and source_data='FT_REFILL') or
(transaction_date='2020-05-18' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-19' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-19' and source_data='FT_REFILL') or
(transaction_date='2020-05-20' and source_data='FT_REFILL') or
(transaction_date='2020-05-20' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-21' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-05-22' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-22' and source_data='FT_REFILL') or
(transaction_date='2020-05-23' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-05-25' and source_data='FT_A_DATA_TRANSFER') or
(transaction_date='2020-05-25' and source_data='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-05-25' and source_data='FT_A_GPRS_ACTIVITY_POST') or
(transaction_date='2020-05-25' and source_data='FT_CREDIT_TRANSFER') or
(transaction_date='2020-05-25' and source_data='FT_CONTRACT_SNAPSHOT') or
(transaction_date='2020-05-25' and source_data='FT_SUBS_RETAIL_ZEBRA') or
(transaction_date='2020-05-25' and source_data='FT_REFILL') or
(transaction_date='2020-05-25' and source_data='FT_A_SUBSCRIPTION') or
(transaction_date='2020-05-25' and source_data='IT_ZTE_ADJUSTMENT') or
(transaction_date='2020-05-25' and source_data='FT_OVERDRAFT') or
(transaction_date='2020-05-25' and source_data='FT_GSM_TRAFFIC_REVENUE_POST') or
(transaction_date='2020-05-25' and source_data='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-05-25' and source_data='FT_EMERGENCY_DATA') or

(transaction_date='2020-05-17' and source_table='FT_ACCOUNT_ACTIVITY') or
(transaction_date='2020-05-17' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or
(transaction_date='2020-05-17' and source_table='FT_A_SUBSCRIBER_SUMMARY') or
(transaction_date='2020-05-18' and source_table='FT_USERS_REGION_LOCATION') or
(transaction_date='2020-05-21' and source_table='FT_USERS_DAY') or
(transaction_date='2020-05-21' and source_table='FT_USERS_REGION_LOCATION') or
(transaction_date='2020-05-21' and source_table='FT_USERS_DATA_DAY') or
(transaction_date='2020-05-22' and source_table='FT_A_SUBSCRIBER_SUMMARY') or
(transaction_date='2020-05-22' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or
(transaction_date='2020-05-22' and source_table='FT_ACCOUNT_ACTIVITY') or
(transaction_date='2020-05-23' and source_table='FT_USERS_DAY') or
(transaction_date='2020-05-23' and source_table='FT_USERS_REGION_LOCATION') or
(transaction_date='2020-05-23' and source_table='FT_USERS_DATA_DAY') or
(transaction_date='2020-05-25' and source_table='FT_USERS_DAY') or
(transaction_date='2020-05-25' and source_table='FT_USERS_DATA_DAY') or
(transaction_date='2020-05-25' and source_table='FT_CONSO_MSISDN_DAY') or
(transaction_date='2020-05-25' and source_table='FT_USERS_REGION_LOCATION') or
(transaction_date='2020-05-25' and source_table='FT_A_GPRS_ACTIVITY') or
(transaction_date='2020-05-25' and source_table='FT_A_GPRS_LOCATION') or
(transaction_date='2020-05-25' and source_table='FT_GROUP_SUBSCRIBER_SUMMARY') or
(transaction_date='2020-05-25' and source_table='FT_GSM_TRAFFIC_REVENUE_DAILY') or
(transaction_date='2020-05-25' and source_table='FT_A_SUBSCRIBER_SUMMARY') or
(transaction_date='2020-05-25' and source_table='FT_ACCOUNT_ACTIVITY') or
(transaction_date='2020-05-25' and source_table='FT_GSM_LOCATION_REVENUE_DAILY') or
(transaction_date='2020-05-25' and source_table='FT_SUBSCRIPTION_SITE_DAY') or
(transaction_date='2020-05-25' and source_table='FT_A_SUBSCRIPTION')
BEGIN
--ALTER SESSION SET NLS_DATE_FORMAT='DD/MM/YYYY';

--Appel des procedures
    P_INS_GSM_TRAFFIC_REVENUE(20, 1);
    COMMIT;
    P_INS_GSM_TRAFFIC_REVENUE_POST(20, 1);
    COMMIT;
    P_INS_SUBSCRIPTION_REVENUE(20, 1);
    COMMIT;
    P_INS_ADJUSTMENT_REVENUE(20, 1);
    COMMIT;
    P_INS_GPRS_REVENUE(20, 1);
    COMMIT;
    P_INS_GPRS_REVENUE_POST(20, 1);
    COMMIT;
    P_INS_SOS_CREDIT_REVENUE(20, 1);
    COMMIT;
    P_INS_SOS_DATA_REVENUE(20, 1);
    COMMIT;
    P_INS_P2P_FEES_REVENUE(20, 1);
    COMMIT;
    P_INS_CREDIT_TRANSFER_TRAFFIC(20, 1);
    COMMIT;
    P_INS_REFILL_TRAFFIC(20, 1);
    COMMIT;
    P_INS_DEAC_ACCT_BAL_REVENUE(20, 1);
    COMMIT;
    P_INS_VAS_DATA_REVENUE(40, 1);
    COMMIT;
    P_INS_P2P_DATA_TRANSF_REVENUE(20, 1);
    COMMIT;

    P_INS_OM_DATA_REVENUE(25, 1);
    COMMIT;

END; -- main

----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_GSM_TRAFFIC_REVENUE (p_gsm_revenu_number_day_from IN NUMBER DEFAULT 15,p_gsm_revenu_number_day_bef IN NUMBER DEFAULT 1) IS

  gsm_revenu_number_day_from number:=p_gsm_revenu_number_day_from; --Nombre de jours à partir duquel il faut checker (J-n)
  gsm_revenu_number_day_bef number:=p_gsm_revenu_number_day_bef; --Nombre de jours à jusqu'à quand il faut checker (Sysdate-n)

  BEGIN

   /* ===>Source Voice SMS
            .Rrevenu Main
  */

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
SELECT     TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              ) TRANSACTION_TYPE
                       ,'MAIN' sub_account
                       ,'+' TRANSACTION_SIGN
                       , SOURCE_PLATFORM
                       ,'FT_GSM_TRAFFIC_REVENUE_DAILY'  SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end) SERVICE_CODE
                       , DESTINATION DESTINATION_CODE
                       , OTHER_PARTY_ZONE SERVED_LOCATION
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END ) MEASUREMENT_UNIT
                       , SUM (TOTAL_COUNT) RATED_COUNT
                       , SUM ( CASE WHEN SERVICE_CODE='VOI_VOX' THEN DURATION
                                    WHEN SERVICE_CODE='NVX_SMS' THEN TOTAL_COUNT
                                    WHEN SERVICE_CODE='NVX_USS' THEN TOTAL_COUNT
                              ELSE TOTAL_COUNT END) RATED_VOLUME
                       , SUM (MAIN_RATED_AMOUNT) TAXED_AMOUNT
                       , SUM (MAIN_RATED_AMOUNT-MAIN_RATED_AMOUNT*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       , 'REVENUE' TRAFFIC_MEAN
                       , OPERATOR_CODE OPERATOR_CODE
                        FROM FT_GSM_TRAFFIC_REVENUE_DAILY
        WHERE
                            TRANSACTION_DATE IN ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_DATA='FT_GSM_TRAFFIC_REVENUE_DAILY' and SUB_ACCOUNT='MAIN' and TRANSACTION_DATE between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef))
                                    )
                            AND NVL(MAIN_RATED_AMOUNT,0)>0
GROUP BY
                        TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              )
                       --, SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end)
                       , DESTINATION
                       ,SOURCE_PLATFORM
                       ,OTHER_PARTY_ZONE
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END )
                       ,OPERATOR_CODE
;
COMMIT;

   /* ===>Source Voice SMS
            .Rrevenu Promo
  */

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
SELECT     TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              ) TRANSACTION_TYPE
                       ,'PROMO' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       , SOURCE_PLATFORM
                       ,'FT_GSM_TRAFFIC_REVENUE_DAILY'  SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end) SERVICE_CODE
                       , DESTINATION DESTINATION_CODE
                       , OTHER_PARTY_ZONE SERVED_LOCATION
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END ) MEASUREMENT_UNIT
                       , SUM (0) RATED_COUNT
                       , SUM (0) RATED_VOLUME
                       , SUM (PROMO_RATED_AMOUNT) TAXED_AMOUNT
                       , SUM (PROMO_RATED_AMOUNT-PROMO_RATED_AMOUNT*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE' TRAFFIC_MEAN
                       , OPERATOR_CODE OPERATOR_CODE
                        FROM FT_GSM_TRAFFIC_REVENUE_DAILY
                        WHERE
                            TRANSACTION_DATE IN ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_DATA='FT_GSM_TRAFFIC_REVENUE_DAILY' and SUB_ACCOUNT='PROMO' and TRANSACTION_DATE between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef))
                                    )
                            AND NVL(PROMO_RATED_AMOUNT,0)>0
                        GROUP BY
                        TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              )
                       --, SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end)
                       , DESTINATION
                       ,SOURCE_PLATFORM
                       ,OTHER_PARTY_ZONE
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END )
                       ,OPERATOR_CODE
                        ;
COMMIT;
END; -- P_INS_GSM_TRAFFIC_REVENUE

----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_GSM_TRAFFIC_REVENUE_POST (p_gsm_revenu_number_day_from IN NUMBER DEFAULT 15,p_gsm_revenu_number_day_bef IN NUMBER DEFAULT 1) IS

  gsm_revenu_number_day_from number:=p_gsm_revenu_number_day_from; --Nombre de jours à partir duquel il faut checker (J-n)
  gsm_revenu_number_day_bef number:=p_gsm_revenu_number_day_bef; --Nombre de jours à jusqu'à quand il faut checker (Sysdate-n)

  BEGIN

   /* ===>Source Voice SMS
            .Rrevenu Main
  */

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
SELECT     TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              ) TRANSACTION_TYPE
                       ,'MAIN' sub_account
                       ,'+' TRANSACTION_SIGN
                       , SOURCE_PLATFORM
                       ,'FT_GSM_TRAFFIC_REVENUE_POST'  SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end) SERVICE_CODE
                       , DESTINATION DESTINATION_CODE
                       , OTHER_PARTY_ZONE SERVED_LOCATION
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END ) MEASUREMENT_UNIT
                       , SUM (TOTAL_COUNT) RATED_COUNT
                       , SUM ( CASE WHEN SERVICE_CODE='VOI_VOX' THEN DURATION
                                    WHEN SERVICE_CODE='NVX_SMS' THEN TOTAL_COUNT
                                    WHEN SERVICE_CODE='NVX_USS' THEN TOTAL_COUNT
                              ELSE TOTAL_COUNT END) RATED_VOLUME
                       , SUM (MAIN_RATED_AMOUNT) TAXED_AMOUNT
                       , SUM (MAIN_RATED_AMOUNT-MAIN_RATED_AMOUNT*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       , 'REVENUE_POST' TRAFFIC_MEAN
                       , OPERATOR_CODE OPERATOR_CODE
                        FROM FT_GSM_TRAFFIC_REVENUE_POST
        WHERE
                            TRANSACTION_DATE IN ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_DATA='FT_GSM_TRAFFIC_REVENUE_POST' and SUB_ACCOUNT='MAIN' and TRANSACTION_DATE between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef))
                                    )
                            AND NVL(MAIN_RATED_AMOUNT,0)>0
GROUP BY
                        TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              )
                       --, SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end)
                       , DESTINATION
                       ,SOURCE_PLATFORM
                       ,OTHER_PARTY_ZONE
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END )
                       ,OPERATOR_CODE
;
COMMIT;

   /* ===>Source Voice SMS
            .Rrevenu Promo
  */

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
SELECT     TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              ) TRANSACTION_TYPE
                       ,'PROMO' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       , SOURCE_PLATFORM
                       ,'FT_GSM_TRAFFIC_REVENUE_POST'  SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end) SERVICE_CODE
                       , DESTINATION DESTINATION_CODE
                       , OTHER_PARTY_ZONE SERVED_LOCATION
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END ) MEASUREMENT_UNIT
                       , SUM (0) RATED_COUNT
                       , SUM (0) RATED_VOLUME
                       , SUM (PROMO_RATED_AMOUNT) TAXED_AMOUNT
                       , SUM (PROMO_RATED_AMOUNT-PROMO_RATED_AMOUNT*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE_POST' TRAFFIC_MEAN
                       , OPERATOR_CODE OPERATOR_CODE
                        FROM FT_GSM_TRAFFIC_REVENUE_POST
                        WHERE
                            TRANSACTION_DATE IN ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_DATA='FT_GSM_TRAFFIC_REVENUE_POST' and SUB_ACCOUNT='PROMO' and TRANSACTION_DATE between trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef))
                                    )
                            AND NVL(PROMO_RATED_AMOUNT,0)>0
                        GROUP BY
                        TRANSACTION_DATE
                       ,OFFER_PROFILE_CODE
                       ,(case when SERVICE_CODE='VOI_VOX' then 'VOICE'
                              when SERVICE_CODE='NVX_SMS' then 'SMS'
                              when SERVICE_CODE='NVX_USS' then 'USSD'
                              ELSE SERVICE_CODE END
                              )
                       --, SOURCE_DATA
                       , SERVED_SERVICE
                       , (case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end)
                       , DESTINATION
                       ,SOURCE_PLATFORM
                       ,OTHER_PARTY_ZONE
                       , (case when SERVICE_CODE='VOI_VOX' then 'DURATION'
                               when SERVICE_CODE='NVX_SMS' then 'HIT'
                               when SERVICE_CODE='NVX_USS' then 'HIT'
                          ELSE 'HIT' END )
                       ,OPERATOR_CODE
                        ;
COMMIT;
END; -- P_INS_GSM_TRAFFIC_REVENUE_POST

----------------------------------------------------------------------------------------------------------------


PROCEDURE P_INS_SUBSCRIPTION_REVENUE (p_subs_number_day_from IN NUMBER DEFAULT 15,p_subs_number_day_bef IN NUMBER DEFAULT 1)  IS

  subs_number_day_from number:=p_subs_number_day_from;
  subs_number_day_bef number:=p_subs_number_day_bef;


  BEGIN
       /* ===>Source Subscription
                .Rrevenu des souscriptions
      */
      /*
  INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT     DATECODE TRANSACTION_DATE
                               ,UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
                               ,(CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP DATA 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP PREPAID 3G%' THEN 'USSD_SUBSCRIPTION' ELSE 'USSD'  END
                               ) TRANSACTION_TYPE
                               ,'MAIN' SUB_ACCOUNT
                               ,'+' TRANSACTION_SIGN
                               , 'ZTE' SOURCE_PLATFORM
                               ,'TT_ZTE_SUBSCRIPTION_LIGHT0'  SOURCE_DATA
                               , 'IN_TRAFFIC' SERVED_SERVICE
                               , (CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP DATA 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP PREPAID 3G%' THEN 'NVX_USS' ELSE SUBS_EVENT_ID  END
                               ) SERVICE_CODE
                               , 'DEST_ND' DESTINATION_CODE
                               , NULL SERVED_LOCATION
                               ,'HIT' MEASUREMENT_UNIT
                               , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
                               , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
                               , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
                               , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
                               , SYSDATE INSERT_DATE
                               ,'REVENUE' TRAFFIC_MEAN
                               , OPERATOR_CODE OPERATOR_CODE
                                FROM (SELECT a.*,b.PRICE_PLAN_NAME FROM TT_ZTE_SUBSCRIPTION_LIGHT0 a,(
          SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE
                    FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT
                    WHERE ORIGINAL_FILE_DATE = (SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT)
          )  b   WHERE a.PRICE_PLAN_CODE=b.PRICE_PLAN_CODE(+) )
                                WHERE
                                    DATECODE IN ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-subs_number_day_from) and trunc(sysdate-subs_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_DATA='TT_ZTE_SUBSCRIPTION_LIGHT0' and TRANSACTION_DATE between trunc(sysdate-subs_number_day_from) and trunc(sysdate-subs_number_day_bef))
                                    )
                                 AND TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
                                 AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
                                GROUP BY
                                 DATECODE
                               ,UPPER(PRICE_PLAN_NAME)
                               ,(CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP DATA 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP PREPAID 3G%' THEN 'USSD_SUBSCRIPTION' ELSE 'USSD'  END
                               )
                               ,'MAIN'
                               ,'+'
                               , 'ZTE'
                               ,'TT_ZTE_SUBSCRIPTION_LIGHT0'
                               , 'IN_TRAFFIC'
                               , (CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP DATA 3G%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                                       OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP PREPAID 3G%' THEN 'NVX_USS' ELSE SUBS_EVENT_ID  END
                               )
                               , 'DEST_ND'
                               , NULL
                               ,'HIT'
                               ,'REVENUE'
                               , OPERATOR_CODE
            */

        -- modifie par dimitri.happi@orange.com le (transaction_date='2020-02-18' and source_data='INSERT') or INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT DATECODE TRANSACTION_DATE
           ,UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
           ,(CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'USSD_SUBSCRIPTION' ELSE 'USSD'  END
           ) TRANSACTION_TYPE
           ,'MAIN' SUB_ACCOUNT
           ,'+' TRANSACTION_SIGN
           , 'ZTE' SOURCE_PLATFORM
           ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
           , 'IN_TRAFFIC' SERVED_SERVICE
           , (CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
                   WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
                   WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
                   WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
                   WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
                   ELSE 'BUN_VOX' /* New individual price plan*/
             END
           ) SERVICE_CODE
           , 'DEST_ND' DESTINATION_CODE
           , NULL SERVED_LOCATION
           ,'HIT' MEASUREMENT_UNIT
           , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
           , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
           , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
           , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
           , SYSDATE INSERT_DATE
           ,'REVENUE' TRAFFIC_MEAN
           , OPERATOR_CODE OPERATOR_CODE
        FROM
        (
            SELECT  TRANSACTION_DATE AS DATECODE,
                SUBS_BENEFIT_NAME AS BENEFIT_NAME,
                COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
                SUBS_EVENT_RATED_COUNT,
                SUBS_AMOUNT AS TOTAL_AMOUNT,
                SUBS_SERVICE,
                OPERATOR_CODE
            FROM FT_A_SUBSCRIPTION
            WHERE TRANSACTION_DATE IN
                    ( select datecode
                      from (select distinct datecode from dim.DT_DATES
                      where datecode between trunc(sysdate-subs_number_day_from) and trunc(sysdate-subs_number_day_bef)
                      minus
                      select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                      where SOURCE_DATA='FT_A_SUBSCRIPTION' and TRANSACTION_DATE between trunc(sysdate-subs_number_day_from) and trunc(sysdate-subs_number_day_bef))
                     ) and subs_channel !='32'
        )
        WHERE
            TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
             AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
        GROUP BY
             DATECODE
            ,UPPER(PRICE_PLAN_NAME)
            ,(CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'USSD_SUBSCRIPTION' ELSE 'USSD'  END
            )
            ,'MAIN'
            ,'+'
            , 'ZTE'
            ,'FT_A_SUBSCRIPTION'
            , 'IN_TRAFFIC'
            , (CASE WHEN NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'DATACM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                   OR  NVL(UPPER(BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
                   WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
                   WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
                   WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
                   WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
                   ELSE 'BUN_VOX' /* New individual price plan*/
             END
            )
            , 'DEST_ND'
            , NULL
            ,'HIT'
            ,'REVENUE'
            , OPERATOR_CODE;

            COMMIT;

    END; -- P_INS_SUBSCRIPTION_REVENUE
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_SOS_CREDIT_REVENUE (p_sos_credit_number_day_from IN NUMBER DEFAULT 15,p_sos_credit_number_day_bef IN NUMBER DEFAULT 1) IS

sos_credit_number_day_from number:=p_sos_credit_number_day_from;
sos_credit_number_day_bef number:=p_sos_credit_number_day_bef;

   /* ===>Source FT_OVERDRAFT
                .Revenu SOS Credit (Revenu généré par les frais de pénalité lié au non rembourssement dans les délais)
     */

    BEGIN
        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
                 SELECT     TRUNC(TRANSACTION_DATE) TRANSACTION_DATE
                               ,UPPER(OFFER_PROFILE_CODE) COMMERCIAL_OFFER_CODE
                               ,'SOS_CREDIT' TRANSACTION_TYPE
                               ,'MAIN' SUB_ACCOUNT
                               ,'-' TRANSACTION_SIGN
                               , 'ZTE' SOURCE_PLATFORM
                               ,'FT_OVERDRAFT'  SOURCE_DATA
                               , 'IN_TRAFFIC' SERVED_SERVICE
                               , 'NVX_SOS' SERVICE_CODE
                               , 'DEST_ND' DESTINATION_CODE
                               , NULL SERVED_LOCATION
                               ,'HIT' MEASUREMENT_UNIT
                               , SUM (1) RATED_COUNT
                               , SUM (1) RATED_VOLUME
                               , SUM (FEE) TAXED_AMOUNT
                               , SUM (FEE-FEE*0.1925) UNTAXED_AMOUNT
                               , SYSDATE INSERT_DATE
                               ,'REVENUE' TRAFFIC_MEAN
                               , OPERATOR_CODE OPERATOR_CODE
                                FROM FT_OVERDRAFT
                                WHERE  TRUNC(TRANSACTION_DATE)IN ( select datecode
                     from (select distinct datecode from dim.DT_DATES
                                 where datecode between trunc(sysdate-sos_credit_number_day_from) and trunc(sysdate-sos_credit_number_day_bef)
                               minus
                          select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                                where TRANSACTION_TYPE='SOS_CREDIT' and TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-sos_credit_number_day_from) AND TRUNC(SYSDATE-sos_credit_number_day_bef))
                                            )
                                 AND NVL(FEE_FLAG,'ND') ='YES' --ne prendre que les évènements qui impliquent une pénalité
                                GROUP BY
                                 TRUNC(TRANSACTION_DATE)
                               ,UPPER(OFFER_PROFILE_CODE)
                               ,'SOS_CREDIT'
                               ,'MAIN'
                               ,'-'
                               , 'ZTE'
                               ,'FT_OVERDRAFT'
                               , 'IN_TRAFFIC'
                               , 'NVX_SOS'
                               , 'DEST_ND'
                               , NULL
                               ,'HIT'
                               ,'REVENUE'
                               , OPERATOR_CODE
                                ;
                COMMIT;
    END; -- P_INS_SOS_CREDIT_REVENUE
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_SOS_DATA_REVENUE (p_sos_data_number_day_from IN NUMBER DEFAULT 15,p_sos_data_number_day_bef IN NUMBER DEFAULT 1) IS

sos_data_number_day_from number:=p_sos_data_number_day_from;
sos_data_number_day_bef number:=p_sos_data_number_day_bef;

   /* ===>Source FT_EMERGENCY_DATA
                .Revenu SOS Data (Revenu généré par les empruntes sur les forfaits data)
     */

    BEGIN
        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
                 SELECT     TRUNC(TRANSACTION_DATE) TRANSACTION_DATE
                               ,UPPER(OFFER_PROFILE_CODE) COMMERCIAL_OFFER_CODE
                               ,'SOS_DATA' TRANSACTION_TYPE
                               ,'MAIN' SUB_ACCOUNT
                               ,'-' TRANSACTION_SIGN
                               , 'ZTE' SOURCE_PLATFORM
                               ,'FT_EMERGENCY_DATA'  SOURCE_DATA
                               , 'IN_TRAFFIC' SERVED_SERVICE
                               , 'NVX_SOS_DATA' SERVICE_CODE
                               , 'DEST_ND' DESTINATION_CODE
                               , NULL SERVED_LOCATION
                               ,'HIT' MEASUREMENT_UNIT
                               , SUM (1) RATED_COUNT
                               , SUM (1) RATED_VOLUME
                               , SUM (AMOUNT) TAXED_AMOUNT
                               , SUM (AMOUNT-AMOUNT*0.1925) UNTAXED_AMOUNT
                               , SYSDATE INSERT_DATE
                               ,'REVENUE' TRAFFIC_MEAN
                               , OPERATOR_CODE OPERATOR_CODE
                                FROM FT_EMERGENCY_DATA
                                WHERE  TRUNC(TRANSACTION_DATE)IN ( select datecode
                     from (select distinct datecode from dim.DT_DATES
                                 where datecode between trunc(sysdate-sos_data_number_day_from) and trunc(sysdate-sos_data_number_day_bef)
                               minus
                          select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                                where TRANSACTION_TYPE='SOS_DATA' and TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-sos_data_number_day_from) AND TRUNC(SYSDATE-sos_data_number_day_bef))
                                            )
                                 AND NVL(TRANSACTION_TYPE,'ND') ='LOAN' --ne prendre que les évènements qui empliquent un emprunte
                                GROUP BY
                                 TRUNC(TRANSACTION_DATE)
                               ,UPPER(OFFER_PROFILE_CODE)
                               ,'SOS_DATA'
                               ,'MAIN'
                               ,'-'
                               , 'ZTE'
                               ,'FT_EMERGENCY_DATA'
                               , 'IN_TRAFFIC'
                               , 'NVX_SOS_DATA'
                               , 'DEST_ND'
                               , NULL
                               ,'HIT'
                               ,'REVENUE'
                               , OPERATOR_CODE
                                ;
                COMMIT;
    END; -- P_INS_SOS_DATA_REVENUE
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_ADJUSTMENT_REVENUE (p_adjustment_day_from IN NUMBER DEFAULT 15,p_adjustment_day_bef IN NUMBER DEFAULT 1) IS

    /* ===>Source Adjustment
                .Revenu CRBT (Telechargement , Abonnement, Souscriptions)
                .Revenu VSMS
                .Revenu des souscriptions au Forfait Data et Autres Souscriptions USSD
                --.Revenu GPRS Pay As You Go NB: (Not use now)
                .Revenu des rachat de validité
     */

adjustment_day_from number:=p_adjustment_day_from;
adjustment_day_bef  number:=p_adjustment_day_bef ;

CURSOR dates_set_to_process IS
   SELECT datecode
     FROM (SELECT DISTINCT datecode FROM dim.DT_DATES
                        WHERE datecode BETWEEN TRUNC(SYSDATE-adjustment_day_from) AND TRUNC(SYSDATE-adjustment_day_bef)
           MINUS
           SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY
                        WHERE SOURCE_DATA='IT_ZTE_ADJUSTMENT'
                        AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-adjustment_day_from) AND TRUNC(SYSDATE-adjustment_day_bef)
          )
;

date_to_process dim.DT_DATES.datecode%TYPE;

       BEGIN
            OPEN dates_set_to_process;
               LOOP   -- loop through the dataset and get each date
                  FETCH dates_set_to_process INTO date_to_process;
                  EXIT WHEN dates_set_to_process%NOTFOUND;
                  IF (MON.FN_IS_ZTE_CDR_CLOSEDv(TO_CHAR(date_to_process,'yyyymmdd'),'ZTE_ADJUSTMENT_CDR'))=1
                      THEN
                          BEGIN
                                INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
                                    SELECT     TRUNC(CREATE_DATE) TRANSACTION_DATE
                                               ,c.PROFILE COMMERCIAL_OFFER_CODE
                                               ,b.GLOBAL_CODE TRANSACTION_TYPE
                                               ,(CASE WHEN ACCT_RES_CODE='1' THEN 'MAIN' ELSE 'PROMO' END) SUB_ACCOUNT
                                               ,'+' TRANSACTION_SIGN
                                               ,'IN' SOURCE_PLATFORM
                                               ,'IT_ZTE_ADJUSTMENT' SOURCE_DATA
                                               ,b.GLOBAL_CODE  SERVED_SERVICE
                                               ,b.GLOBAL_USAGE_CODE SERVICE_CODE
                                               , 'DEST_ND' DESTINATION_CODE
                                               , NULL SERVED_LOCATION
                                               , NULL MEASUREMENT_UNIT
                                               , SUM(CASE WHEN CHARGE>0 THEN 1 ELSE 0 END) RATED_COUNT
                                               , SUM(CASE WHEN CHARGE>0 THEN 1 ELSE 0 END) RATED_VOLUME
                                               , SUM(CASE WHEN CHARGE>0 THEN CHARGE/100 ELSE 0 END) TAXED_AMOUNT
                                               , SUM(CASE WHEN CHARGE>0 THEN  (CHARGE/100)-(CHARGE/100)*0.1925 ELSE 0 END) UNTAXED_AMOUNT
                                               , SYSDATE INSERT_DATE
                                               ,'REVENUE' TRAFFIC_MEAN
                                               , MON.FN_GET_OPERATOR_CODE(LTRIM(ACC_NBR,'237')) OPERATOR_CODE
                                               FROM CDR.IT_ZTE_ADJUSTMENT a,DIM.DT_ZTE_USAGE_TYPE b,(SELECT * FROM FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE=(SELECT MAX(EVENT_DATE) EVENT_DATE FROM FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE>=TO_DATE (TO_CHAR(date_to_process,'YYYYMMDD'),'YYYYMMDD')-31) ) c
                                               WHERE a.CHANNEL_ID=b.USAGE_CODE
                                               AND b.FLUX_SOURCE='ADJUSTMENT'
                                               AND CREATE_DATE BETWEEN TO_DATE (TO_CHAR(date_to_process,'YYYYMMDD')||' 000000','YYYYMMDD HH24MISS') AND TO_DATE (TO_CHAR(date_to_process,'YYYYMMDD')||' 235959','YYYYMMDD HH24MISS')
                                               AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37')--'16' ne pas prendre le GPRS ici,26=cra de parrainage,'28' =celebrite
                                               AND LTRIM(a.ACC_NBR,'237')=c.ACCESS_KEY(+)
                                       GROUP BY TRUNC(CREATE_DATE)
                                               ,NULL
                                               ,b.GLOBAL_CODE
                                               ,(CASE WHEN ACCT_RES_CODE='1' THEN 'MAIN' ELSE 'PROMO' END)
                                               ,'+'
                                               ,'IN'
                                               ,'IT_ZTE_ADJUSTMENT'
                                               ,b.GLOBAL_CODE
                                               ,b.GLOBAL_USAGE_CODE
                                               ,c.PROFILE
                                               , 'DEST_ND'
                                               , NULL
                                               , NULL
                                               ,MON.FN_GET_OPERATOR_CODE(LTRIM(ACC_NBR,'237'))
                                               ;
                                    COMMIT;
                            END;
                   END IF;
              END LOOP;
          CLOSE dates_set_to_process;
     END;  -- P_INS_ADJUSTMENT_REVENUE
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_P2P_FEES_REVENUE (p_p2p_number_day_from IN NUMBER DEFAULT 15,p_p2p_number_day_bef IN NUMBER DEFAULT 1)  IS

  p2p_number_day_from number:=p_p2p_number_day_from;
  p2p_number_day_bef number:=p_p2p_number_day_bef;

  BEGIN
    /* ===>Source FT_CREDIT_TRANSFER
                .Revenu des frais de transfert de crédit
     */
  INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
         SELECT   REFILL_DATE TRANSACTION_DATE
                       ,COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
                       ,'P2P_FEES' TRANSACTION_TYPE
                       ,'MAIN' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       ,'P2P' SOURCE_PLATFORM
                       , 'FT_CREDIT_TRANSFER' SOURCE_DATA
                       , 'P2P_TRAFFIC' SERVED_SERVICE
                       , 'NVX_P2P' SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , 'HIT' MEASUREMENT_UNIT
                       , SUM (1) RATED_COUNT
                       , SUM (1) RATED_VOLUME
                       , SUM (TRANSFER_FEES) TAXED_AMOUNT
                       , SUM (TRANSFER_FEES-TRANSFER_FEES*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE' TRAFFIC_MEAN
                       , MON.FN_GET_OPERATOR_CODE(SENDER_MSISDN) OPERATOR_CODE
                        FROM  MON.FT_CREDIT_TRANSFER
                        WHERE
                            refill_date in ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-p2p_number_day_from) and trunc(sysdate-p2p_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where TRANSACTION_TYPE='P2P_FEES' and TRANSACTION_DATE between trunc(sysdate-p2p_number_day_from) and trunc(sysdate-p2p_number_day_bef))
                                    )
                 and termination_ind='000'
                        GROUP BY
                        REFILL_DATE
                       ,COMMERCIAL_OFFER
                       ,MON.FN_GET_OPERATOR_CODE(SENDER_MSISDN)
                       ;
                       COMMIT;
    END; -- P_INS_P2P_FEES_REVENUE
----------------------------------------------------------------------------------------------------------------


PROCEDURE P_INS_DEAC_ACCT_BAL_REVENUE (p_deac_acc_bal_number_day_from IN NUMBER DEFAULT 15,p_deac_acc_bal_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source CONTRACT_SNAPSHOT
                .Revenu des crédits en compte des lignes désactivées

                -- Compte MAIN
     */

deac_acct_bal_number_day_from number:=p_deac_acc_bal_number_day_from;
deac_acc_bal_number_day_bef number:=p_deac_acc_bal_number_day_bef;






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
