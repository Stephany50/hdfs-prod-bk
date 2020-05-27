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
        select
            msisdn,
            region,
            location_ci,
            served_party_location,
            duree_sortant,
            duree_entrant,
            nbre_sms_sortant,
            nbre_sms_entrant,
            Nvl (duree_sortant, 0) + Nvl ( duree_entrant, 0) + Nvl (nbre_sms_sortant, 0) + Nvl (nbre_sms_entrant, 0) traffic
        from (
            select
                fn_format_msisdn_to_9digits(served_msisdn) msisdn ,
                served_party_location,
                substring(served_party_location,14,5) location_ci,
                Sum (CASE
                      WHEN a.transaction_direction = 'Sortant'
                           AND Substr (transaction_type, 1, 3) <> 'SMS' THEN
                      transaction_duration
                      ELSE 0
                    END) DUREE_SORTANT,
               Sum (CASE
                      WHEN a.transaction_direction = 'Sortant'
                           AND Substr (transaction_type, 1, 3) <> 'SMS' THEN 1
                      ELSE 0
                    END)  NBRE_TEL_SORTANT,
               Sum (CASE
                      WHEN a.transaction_direction = 'Entrant'
                           AND Substr (transaction_type, 1, 3) <> 'SMS' THEN
                      transaction_duration
                      ELSE 0
                    END)  DUREE_ENTRANT,
               Sum (CASE
                      WHEN a.transaction_direction = 'Entrant'
                           AND Substr (transaction_type, 1, 3) <> 'SMS' THEN 1
                      ELSE 0
                    END)  NBRE_TEL_ENTRANT,
               Sum (CASE
                      WHEN a.transaction_direction = 'Sortant'
                           AND Substr (transaction_type, 1, 3) = 'SMS' THEN 1
                      ELSE 0
                    END) NBRE_SMS_SORTANT,
               Sum (CASE
                      WHEN a.transaction_direction = 'Entrant'
                           AND Substr (transaction_type, 1, 3) = 'SMS' THEN 1
                      ELSE 0
                    END) nbre_sms_entrant
            from mon.spark_ft_msc_transaction a
            where transaction_date>=date_sub(current_date,30)  and served_party_location LIKE '624-02-%'
            group by
            fn_format_msisdn_to_9digits(served_msisdn),
            served_party_location,
            substring(served_party_location,14,5)
        )t
        left join dim.dt_gsm_cell_code on t.location_ci=ci where ci is not null
    )t
)t
where rang=1
--group by msisdn




CREATE OR REPLACE PACKAGE BODY MON.PCK_LOAD_GLOBAL_ACTIVITY IS
            /*
            <VERSION>1.0.0</VERSION>
            <FILENAME>MON.PCK_LOAD_GLOBAL_ACTIVITY.pkg</FILENAME>
            <AUTHOR>Felix BOGNOU felix.bognou@orange.cm</AUTHOR>
            <SUMMARY>none</SUMMARY>
            <COPYRIGHT>Orange Cameroun, 2013</COPYRIGHT>
            <OVERVIEW>
                Ce Package alimente l'agregat GLOBAL_ACTIVITY qui contient l'activité glogale des abonnés Orange Cameroun
            </OVERVIEW>
            <DEPENDENCIES>
            ALL_SOURCE data dictionary view
            </DEPENDENCIES>
            <EXCEPTIONS>None</EXCEPTIONS>
            Modification History
            Date By Modification
            ---------- --------- -------------------------------
            <MODIFICATIONS>
            10/05/2013 Creation date
            </MODIFICATIONS>
            */
PROCEDURE MAIN IS
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

        -- modifie par dimitri.happi@orange.com le 18/02/2016
        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
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

    BEGIN

            INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
            SELECT     DEACTIVATION_DATE TRANSACTION_DATE
                                   ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
                                   ,'DEACTIVATED_ACCOUNT_BALANCE' TRANSACTION_TYPE
                                   ,'MAIN' SUB_ACCOUNT
                                   ,'-' TRANSACTION_SIGN
                                   , 'IN' SOURCE_PLATFORM
                                   ,'FT_CONTRACT_SNAPSHOT'  SOURCE_DATA
                                   , 'IN_ACCOUNT' SERVED_SERVICE
                                   , 'NVX_BALANCE' SERVICE_CODE
                                   , 'DEST_ND' DESTINATION_CODE
                                   , NULL SERVED_LOCATION
                                   ,'HIT' MEASUREMENT_UNIT
                                   , SUM (1) RATED_COUNT
                                   , SUM (1) RATED_VOLUME
                                   , SUM (MAIN_CREDIT) TAXED_AMOUNT
                                   , SUM (MAIN_CREDIT-MAIN_CREDIT*0.1925) UNTAXED_AMOUNT
                                   , SYSDATE INSERT_DATE
                                   ,'REVENUE' TRAFFIC_MEAN
                                   , FN_GET_OPERATOR_CODE(ACCESS_KEY) OPERATOR_CODE
                                    FROM FT_CONTRACT_SNAPSHOT
                                    WHERE  EVENT_DATE=TRUNC(SYSDATE-deac_acc_bal_number_day_bef)
                                    AND DEACTIVATION_DATE  IN ( select datecode
                         from (select distinct datecode from dim.DT_DATES
                                     where datecode between trunc(sysdate-deac_acct_bal_number_day_from) and trunc(sysdate-deac_acc_bal_number_day_bef)
                                   minus
                              select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                                    where TRANSACTION_TYPE='DEACTIVATED_ACCOUNT_BALANCE' and SUB_ACCOUNT='MAIN' and TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-deac_acct_bal_number_day_from) AND TRUNC(SYSDATE-deac_acc_bal_number_day_bef))
                                                )
                                    AND DEACTIVATION_DATE IS NOT NULL
                                    GROUP BY
                                      DEACTIVATION_DATE
                                   ,UPPER(PROFILE)
                                   ,'DEACTIVATED_ACCOUNT_BALANCE'
                                   ,'MAIN'
                                   ,'-'
                                   , 'IN'
                                   ,'FT_CONTRACT_SNAPSHOT'
                                   , 'IN_ACCOUNT'
                                   , 'NVX_BALANCE'
                                   , 'DEST_ND'
                                   , NULL
                                   ,'HIT'
                                   ,'REVENUE'
                                   , FN_GET_OPERATOR_CODE(ACCESS_KEY)
                                    ;
            COMMIT;

    /* ===>Source CONTRACT_SNAPSHOT
                .Revenu des crédits en compte des lignes désactivées

                -- Compte PROMO
     */

        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT     DEACTIVATION_DATE TRANSACTION_DATE
                               ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
                               ,'DEACTIVATED_ACCOUNT_BALANCE' TRANSACTION_TYPE
                               ,'PROMO' SUB_ACCOUNT
                               ,'-' TRANSACTION_SIGN
                               , 'IN' SOURCE_PLATFORM
                               ,'FT_CONTRACT_SNAPSHOT'  SOURCE_DATA
                               , 'IN_ACCOUNT' SERVED_SERVICE
                               , 'NVX_BALANCE' SERVICE_CODE
                               , 'DEST_ND' DESTINATION_CODE
                               , NULL SERVED_LOCATION
                               ,'HIT' MEASUREMENT_UNIT
                               , SUM (1) RATED_COUNT
                               , SUM (1) RATED_VOLUME
                               , SUM (PROMO_CREDIT) TAXED_AMOUNT
                               , SUM (PROMO_CREDIT-PROMO_CREDIT*0.1925) UNTAXED_AMOUNT
                               , SYSDATE INSERT_DATE
                               ,'REVENUE' TRAFFIC_MEAN
                               , FN_GET_OPERATOR_CODE(ACCESS_KEY) OPERATOR_CODE
                                FROM FT_CONTRACT_SNAPSHOT
                                WHERE  EVENT_DATE=TRUNC(SYSDATE-deac_acc_bal_number_day_bef)
                                AND DEACTIVATION_DATE  IN ( select datecode
                     from (select distinct datecode from dim.DT_DATES
                                 where datecode between trunc(sysdate-deac_acct_bal_number_day_from) and trunc(sysdate-deac_acc_bal_number_day_bef)
                               minus
                          select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                                where TRANSACTION_TYPE='DEACTIVATED_ACCOUNT_BALANCE' and SUB_ACCOUNT='PROMO' and TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-deac_acct_bal_number_day_from) AND TRUNC(SYSDATE-deac_acc_bal_number_day_bef))
                                            )
                                AND DEACTIVATION_DATE IS NOT NULL
                                GROUP BY
                                  DEACTIVATION_DATE
                               ,UPPER(PROFILE)
                               ,'DEACTIVATED_ACCOUNT_BALANCE'
                               ,'PROMO'
                               ,'-'
                               , 'IN'
                               ,'FT_CONTRACT_SNAPSHOT'
                               , 'IN_ACCOUNT'
                               , 'NVX_BALANCE'
                               , 'DEST_ND'
                               , NULL
                               ,'HIT'
                               ,'REVENUE'
                               , FN_GET_OPERATOR_CODE(ACCESS_KEY)
                                ;
                    COMMIT;
 END; -- P_INS_DEAC_ACCT_BAL_REVENUE
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_GPRS_REVENUE (p_gprs_number_day_from IN NUMBER DEFAULT 15,p_gprs_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source FT_A_GPRS_ACTIVITY
                .Revenu du tafic GPRS (Pay as you Go pour les prepayés uniquement)
                -- Compte MAIN -(MAJ le 17/11/2014 suite promo super Bonus avec utilisation du credit promo en pay as you go
     */

  gprs_number_day_from number:=p_gprs_number_day_from;
  gprs_number_day_bef number:=p_gprs_number_day_bef;

  BEGIN
    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
    SELECT     DATECODE TRANSACTION_DATE
                       ,UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                       ,'GPRS' TRANSACTION_TYPE
                       ,'MAIN' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       ,'VOLUBILL' SOURCE_PLATFORM
                       ,'FT_A_GPRS_ACTIVITY' SOURCE_DATA
                       ,'GPRS_TRAFFIC' SERVED_SERVICE
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , MEASUREMENT_UNIT MEASUREMENT_UNIT
                       , SUM(DECODE(MAIN_COST,0,0,1)) RATED_COUNT
                       , SUM(DECODE(MAIN_COST,0,0,TOTAL_UNIT)) RATED_VOLUME
                       , SUM(MAIN_COST) TAXED_AMOUNT
                       , SUM (MAIN_COST-MAIN_COST*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE' TRAFFIC_MEAN
                       , a.OPERATOR_CODE OPERATOR_CODE
                        FROM  MON.FT_A_GPRS_ACTIVITY a
                        WHERE MAIN_COST>0
                        AND DATECODE IN  ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gprs_number_day_from) and trunc(sysdate-gprs_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where TRANSACTION_TYPE='GPRS' and SOURCE_DATA='FT_A_GPRS_ACTIVITY' and SUB_ACCOUNT='MAIN' and TRANSACTION_DATE between trunc(sysdate-gprs_number_day_from) and trunc(sysdate-gprs_number_day_bef))
                                    )
                        GROUP BY
                        DATECODE
                       ,UPPER(COMMERCIAL_OFFER)
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
                       , MEASUREMENT_UNIT
                       , a.OPERATOR_CODE
                ;
        COMMIT;
     --Sous compte promo (MAJ le 17/11/2014 suite promo super Bonus avec utilisation du credit promo en pay as you go
        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
    SELECT     DATECODE TRANSACTION_DATE
                       ,UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                       ,'GPRS' TRANSACTION_TYPE
                       ,'PROMO' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       ,'VOLUBILL' SOURCE_PLATFORM
                       ,'FT_A_GPRS_ACTIVITY' SOURCE_DATA
                       ,'GPRS_TRAFFIC' SERVED_SERVICE
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , MEASUREMENT_UNIT MEASUREMENT_UNIT
                       , SUM(DECODE(PROMO_COST,0,0,1)) RATED_COUNT
                       , SUM(DECODE(PROMO_COST,0,0,TOTAL_UNIT)) RATED_VOLUME
                       , SUM(PROMO_COST) TAXED_AMOUNT
                       , SUM(PROMO_COST-PROMO_COST*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE' TRAFFIC_MEAN
                       , a.OPERATOR_CODE OPERATOR_CODE
                        FROM  MON.FT_A_GPRS_ACTIVITY a
                        WHERE PROMO_COST>0
                        AND DATECODE IN  ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gprs_number_day_from) and trunc(sysdate-gprs_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where TRANSACTION_TYPE='GPRS' and SOURCE_DATA='FT_A_GPRS_ACTIVITY' and SUB_ACCOUNT='PROMO' and TRANSACTION_DATE between trunc(sysdate-gprs_number_day_from) and trunc(sysdate-gprs_number_day_bef))
                                    )
                        GROUP BY
                        DATECODE
                       ,UPPER(COMMERCIAL_OFFER)
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
                       , MEASUREMENT_UNIT
                       , a.OPERATOR_CODE
                ;
        COMMIT;
    END; -- P_INS_GPRS_REVENUE
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_GPRS_REVENUE_POST (p_gprs_post_number_day_from IN NUMBER DEFAULT 15,p_gprs_post_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source FT_A_GPRS_ACTIVITY_POST
                .Revenu du tafic GPRS (Pay as you Go pour les prepayés uniquement)
                -- Compte MAIN -(MAJ le 17/11/2014 suite promo super Bonus avec utilisation du credit promo en pay as you go
     */

  gprs_post_number_day_from number:=p_gprs_post_number_day_from;
  gprs_post_number_day_bef number:=p_gprs_post_number_day_bef;

  BEGIN
    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
    SELECT     DATECODE TRANSACTION_DATE
                       ,UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                       ,'GPRS' TRANSACTION_TYPE
                       ,'MAIN' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       ,'VOLUBILL' SOURCE_PLATFORM
                       ,'FT_A_GPRS_ACTIVITY_POST' SOURCE_DATA
                       ,'GPRS_TRAFFIC' SERVED_SERVICE
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , MEASUREMENT_UNIT MEASUREMENT_UNIT
                       , SUM(DECODE(MAIN_COST,0,0,1)) RATED_COUNT
                       , SUM(DECODE(MAIN_COST,0,0,TOTAL_UNIT)) RATED_VOLUME
                       , SUM(MAIN_COST) TAXED_AMOUNT
                       , SUM (MAIN_COST-MAIN_COST*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE_POST' TRAFFIC_MEAN
                       , a.OPERATOR_CODE OPERATOR_CODE
                        FROM  MON.FT_A_GPRS_ACTIVITY_POST a
                        WHERE MAIN_COST>0
                        AND DATECODE IN  ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gprs_post_number_day_from) and trunc(sysdate-gprs_post_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where TRANSACTION_TYPE='GPRS' and SOURCE_DATA='FT_A_GPRS_ACTIVITY_POST' and SUB_ACCOUNT='MAIN' and TRANSACTION_DATE between trunc(sysdate-gprs_post_number_day_from) and trunc(sysdate-gprs_post_number_day_bef))
                                    )
                        GROUP BY
                        DATECODE
                       ,UPPER(COMMERCIAL_OFFER)
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
                       , MEASUREMENT_UNIT
                       , a.OPERATOR_CODE
                ;
        COMMIT;
     --Sous compte promo (MAJ le 17/11/2014 suite promo super Bonus avec utilisation du credit promo en pay as you go
        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
    SELECT     DATECODE TRANSACTION_DATE
                       ,UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                       ,'GPRS' TRANSACTION_TYPE
                       ,'PROMO' SUB_ACCOUNT
                       ,'+' TRANSACTION_SIGN
                       ,'VOLUBILL' SOURCE_PLATFORM
                       ,'FT_A_GPRS_ACTIVITY_POST' SOURCE_DATA
                       ,'GPRS_TRAFFIC' SERVED_SERVICE
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , MEASUREMENT_UNIT MEASUREMENT_UNIT
                       , SUM(DECODE(PROMO_COST,0,0,1)) RATED_COUNT
                       , SUM(DECODE(PROMO_COST,0,0,TOTAL_UNIT)) RATED_VOLUME
                       , SUM(PROMO_COST) TAXED_AMOUNT
                       , SUM(PROMO_COST-PROMO_COST*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'REVENUE_POST' TRAFFIC_MEAN
                       , a.OPERATOR_CODE OPERATOR_CODE
                        FROM  MON.FT_A_GPRS_ACTIVITY_POST a
                        WHERE PROMO_COST>0
                        AND DATECODE IN  ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-gprs_post_number_day_from) and trunc(sysdate-gprs_post_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where TRANSACTION_TYPE='GPRS' and SOURCE_DATA='FT_A_GPRS_ACTIVITY_POST' and SUB_ACCOUNT='PROMO' and TRANSACTION_DATE between trunc(sysdate-gprs_post_number_day_from) and trunc(sysdate-gprs_post_number_day_bef))
                                    )
                        GROUP BY
                        DATECODE
                       ,UPPER(COMMERCIAL_OFFER)
                       ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
                       , MEASUREMENT_UNIT
                       , a.OPERATOR_CODE
                ;
        COMMIT;
    END; -- P_INS_GPRS_REVENUE_POST
----------------------------------------------------------------------------------------------------------------



PROCEDURE P_INS_CREDIT_TRANSFER_TRAFFIC (p_p2p_number_day_from IN NUMBER DEFAULT 15,p_p2p_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source FT_CREDIT_TRANSFER
                .Tafic FT_CREDIT_TRANSFER
     */

  p2p_number_day_from number:=p_p2p_number_day_from;
  p2p_number_day_bef number:=p_p2p_number_day_bef;

  BEGIN

  INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT     REFILL_DATE TRANSACTION_DATE
                       ,COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
                       ,'P2P' TRANSACTION_TYPE
                       ,'MAIN' SUB_ACCOUNT
                       ,'0' TRANSACTION_SIGN
                       ,'P2P' SOURCE_PLATFORM
                       , 'FT_CREDIT_TRANSFER' SOURCE_DATA
                       , 'P2P_TRAFFIC' SERVED_SERVICE
                       , 'NVX_P2P_TRANS' SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , 'HIT' MEASUREMENT_UNIT
                       , SUM (1) RATED_COUNT
                       , SUM (1) RATED_VOLUME
                       , SUM (TRANSFER_AMT) TAXED_AMOUNT
                       , SUM (TRANSFER_AMT-TRANSFER_AMT*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'AIRTIME' TRAFFIC_MEAN
                       , MON.FN_GET_OPERATOR_CODE(SENDER_MSISDN) OPERATOR_CODE
                        FROM  MON.FT_CREDIT_TRANSFER
                        WHERE
                            refill_date in ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-p2p_number_day_from) and trunc(sysdate-p2p_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where TRANSACTION_TYPE='P2P' and TRANSACTION_DATE between trunc(sysdate-p2p_number_day_from) and trunc(sysdate-p2p_number_day_bef))
                                    )
                 and termination_ind='000'
                        GROUP BY
                        REFILL_DATE
                       ,COMMERCIAL_OFFER
                       ,MON.FN_GET_OPERATOR_CODE(SENDER_MSISDN)
                       ;
              COMMIT;
    END; -- P_INS_CREDIT_TRANSFER_TRAFFIC
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_REFILL_TRAFFIC (p_refill_number_day_from IN NUMBER DEFAULT 15,p_refill_number_day_bef IN NUMBER DEFAULT 1)  IS

refill_number_day_from number:=p_refill_number_day_from;
refill_number_day_bef number:=p_refill_number_day_bef;

  BEGIN
  -- Trafic Recharge
      /*Recharge Valeur faciale*/
  INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT     REFILL_DATE TRANSACTION_DATE
                       ,RECEIVER_PROFILE COMMERCIAL_OFFER_CODE
                       ,(Case when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
                              when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
                              when mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' then 'RECHARGE_WHA'
                              when mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'TRANSFERT_WHA'
                              when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)<>'694010631' and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)<>'694010631' then 'VALEUR_FACIALE'
                         else REFILL_MEAN end) TRANSACTION_TYPE
                       ,(case when REFILL_MEAN='SCRATCH' then 'MAIN'
                              when REFILL_MEAN='C2S' then 'MAIN'
                              when REFILL_MEAN in ('C2C','O2C') and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'WHA'
                       else 'ZEBRA' end) SUB_ACCOUNT
                       ,'0' TRANSACTION_SIGN
                       ,(case when REFILL_MEAN='SCRATCH' then 'ICC' else 'ZEBRA' end ) SOURCE_PLATFORM
                       , 'FT_REFILL' SOURCE_DATA
                       , (case when REFILL_MEAN='SCRATCH' then 'ICC_TRAFFIC' else  'ZEBRA_TRAFFIC' end) SERVED_SERVICE
                       , (Case when (mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' or mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631') then 'NVX_WHA'
                               else DECODE(REFILL_MEAN,'C2S','NVX_C2S','C2C','NVX_C2C','O2C','NVX_O2C','SCRATCH','NVX_TOPUP',REFILL_MEAN)
                                 end   ) SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , 'HIT' MEASUREMENT_UNIT
                       , SUM (1) RATED_COUNT
                       , SUM (1) RATED_VOLUME
                       , SUM (case when refill_type='RETURN' then  -REFILL_AMOUNT else REFILL_AMOUNT end) TAXED_AMOUNT
                       , SUM (case when refill_type='RETURN' then  0 else REFILL_AMOUNT-REFILL_AMOUNT*0.1925 end) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'AIRTIME' TRAFFIC_MEAN
                       , MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN)  OPERATOR_CODE
                        FROM  MON.FT_REFILL
                        WHERE
                            REFILL_MEAN='SCRATCH' and refill_date in ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SERVICE_CODE='NVX_TOPUP' and TRANSACTION_TYPE<>'BONUS_RECHARGE' and TRANSACTION_DATE between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef))
                                    )
                 and termination_ind='200'
                        GROUP BY
                        REFILL_DATE
                       ,RECEIVER_PROFILE
                       ,(Case when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
                              when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
                              when mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' then 'RECHARGE_WHA'
                              when mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'TRANSFERT_WHA'
                              when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)<>'694010631' and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)<>'694010631' then 'VALEUR_FACIALE'
                         else REFILL_MEAN end)
                       ,(case when REFILL_MEAN='SCRATCH' then 'MAIN'
                              when REFILL_MEAN='C2S' then 'MAIN'
                              when REFILL_MEAN in ('C2C','O2C') and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'WHA'
                       else 'ZEBRA' end)
                       ,(case when REFILL_MEAN='SCRATCH' then 'ICC' else 'ZEBRA' end )
                       , (case when REFILL_MEAN='SCRATCH' then 'ICC_TRAFFIC' else  'ZEBRA_TRAFFIC' end)
                       , (Case when (mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' or mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631') then 'NVX_WHA'
                               else DECODE(REFILL_MEAN,'C2S','NVX_C2S','C2C','NVX_C2C','O2C','NVX_O2C','SCRATCH','NVX_TOPUP',REFILL_MEAN)
                                 end   )
                       , MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN)
                       ;
                       COMMIT;

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
       SELECT     REFILL_DATE TRANSACTION_DATE
                       ,RECEIVER_PROFILE COMMERCIAL_OFFER_CODE
                       ,(Case when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
                              when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
                              when mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' then 'RECHARGE_WHA'
                              when mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'TRANSFERT_WHA'
                              when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)<>'694010631' and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)<>'694010631' then 'VALEUR_FACIALE'
                         else REFILL_MEAN end) TRANSACTION_TYPE
                       ,(case when REFILL_MEAN='SCRATCH' then 'MAIN'
                              when REFILL_MEAN='C2S' then 'MAIN'
                              when REFILL_MEAN in ('C2C','O2C') and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'WHA'
                       else 'ZEBRA' end) SUB_ACCOUNT
                       ,'0' TRANSACTION_SIGN
                       ,(case when REFILL_MEAN='SCRATCH' then 'ICC' else 'ZEBRA' end ) SOURCE_PLATFORM
                       , 'FT_REFILL' SOURCE_DATA
                       , (case when REFILL_MEAN='SCRATCH' then 'ICC_TRAFFIC' else  'ZEBRA_TRAFFIC' end) SERVED_SERVICE
                       , (Case when (mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' or mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631') then 'NVX_WHA'
                               else DECODE(REFILL_MEAN,'C2S','NVX_C2S','C2C','NVX_C2C','O2C','NVX_O2C','SCRATCH','NVX_TOPUP',REFILL_MEAN)
                                 end   ) SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , 'HIT' MEASUREMENT_UNIT
                       , SUM (1) RATED_COUNT
                       , SUM (1) RATED_VOLUME
                       , SUM (case when refill_type='RETURN' then  -REFILL_AMOUNT else REFILL_AMOUNT end) TAXED_AMOUNT
                       , SUM (case when refill_type='RETURN' then  0 else REFILL_AMOUNT-REFILL_AMOUNT*0.1925 end) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'AIRTIME' TRAFFIC_MEAN
                       , MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN) OPERATOR_CODE
                        FROM  MON.FT_REFILL
                        WHERE
                            REFILL_MEAN<>'SCRATCH' and refill_date in ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_PLATFORM='ZEBRA' and TRANSACTION_TYPE<>'BONUS_RECHARGE' and TRANSACTION_DATE between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef))
                                    )
                 and termination_ind='200'
                        GROUP BY
                        REFILL_DATE
                       ,RECEIVER_PROFILE
                       ,(Case when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
                              when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
                              when mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' then 'RECHARGE_WHA'
                              when mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'TRANSFERT_WHA'
                              when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)<>'694010631' and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)<>'694010631' then 'VALEUR_FACIALE'
                         else REFILL_MEAN end)
                       ,(case when REFILL_MEAN='SCRATCH' then 'MAIN'
                              when REFILL_MEAN='C2S' then 'MAIN'
                              when REFILL_MEAN in ('C2C','O2C') and mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631' then 'WHA'
                       else 'ZEBRA' end)
                       ,(case when REFILL_MEAN='SCRATCH' then 'ICC' else 'ZEBRA' end )
                       , (case when REFILL_MEAN='SCRATCH' then 'ICC_TRAFFIC' else  'ZEBRA_TRAFFIC' end)
                       , (Case when (mon.fn_format_msisdn_to_9digits(SENDER_MSISDN)='694010631' or mon.fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='694010631') then 'NVX_WHA'
                               else DECODE(REFILL_MEAN,'C2S','NVX_C2S','C2C','NVX_C2C','O2C','NVX_O2C','SCRATCH','NVX_TOPUP',REFILL_MEAN)
                                 end   )
                       , MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN)
                       ;
              COMMIT;

/*Recharge Valeur bonus*/
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT     REFILL_DATE TRANSACTION_DATE
                       ,RECEIVER_PROFILE COMMERCIAL_OFFER_CODE
                       ,'BONUS_RECHARGE' TRANSACTION_TYPE
                       ,'PROMO' SUB_ACCOUNT
                       ,'0' TRANSACTION_SIGN
                       ,(case when refill_mean='SCRATCH' then 'ICC' else 'ZEBRA' end ) SOURCE_PLATFORM
                       , 'FT_REFILL' SOURCE_DATA
                       ,(case when refill_mean='SCRATCH' then 'ICC_TRAFFIC' else 'ZEBRA_TRAFFIC' end ) SERVED_SERVICE
                       ,(case when refill_mean='SCRATCH' then 'NVX_TOPUP' else 'NVX_C2S' end )  SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , 'HIT' MEASUREMENT_UNIT
                       , sum(0) RATED_COUNT
                       , sum(0) RATED_VOLUME
                       , SUM (REFILL_BONUS) TAXED_AMOUNT
                       , SUM (REFILL_BONUS-REFILL_BONUS*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'AIRTIME' TRAFFIC_MEAN
                       , MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN) OPERATOR_CODE
                        FROM  MON.FT_REFILL
                        WHERE
                           REFILL_MEAN='SCRATCH' and refill_date in ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SERVICE_CODE='NVX_TOPUP' and TRANSACTION_TYPE='BONUS_RECHARGE' and TRANSACTION_DATE between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef))
                                    )
                 and termination_ind='200'
                 and REFILL_MEAN in ('C2S','SCRATCH')
                        GROUP BY
                        REFILL_DATE
                       ,RECEIVER_PROFILE
                       ,MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN)
                       ,(case when refill_mean='SCRATCH' then 'ICC' else 'ZEBRA' end )
                       ,(case when refill_mean='SCRATCH' then 'ICC_TRAFFIC' else 'ZEBRA_TRAFFIC' end )
                       ,(case when refill_mean='SCRATCH' then 'NVX_TOPUP' else 'NVX_C2S' end )
                       ;
             COMMIT;

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
       SELECT     REFILL_DATE TRANSACTION_DATE
                       ,RECEIVER_PROFILE COMMERCIAL_OFFER_CODE
                       ,'BONUS_RECHARGE' TRANSACTION_TYPE
                       ,'PROMO' SUB_ACCOUNT
                       ,'0' TRANSACTION_SIGN
                       ,(case when refill_mean='SCRATCH' then 'ICC' else 'ZEBRA' end ) SOURCE_PLATFORM
                       , 'FT_REFILL' SOURCE_DATA
                       ,(case when refill_mean='SCRATCH' then 'ICC_TRAFFIC' else 'ZEBRA_TRAFFIC' end ) SERVED_SERVICE
                       ,(case when refill_mean='SCRATCH' then 'NVX_TOPUP' else 'NVX_C2S' end )  SERVICE_CODE
                       , 'DEST_ND' DESTINATION_CODE
                       , NULL SERVED_LOCATION
                       , 'HIT' MEASUREMENT_UNIT
                       , sum(0) RATED_COUNT
                       , sum(0) RATED_VOLUME
                       , SUM (REFILL_BONUS) TAXED_AMOUNT
                       , SUM (REFILL_BONUS-REFILL_BONUS*0.1925) UNTAXED_AMOUNT
                       , SYSDATE INSERT_DATE
                       ,'AIRTIME' TRAFFIC_MEAN
                       , MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN) OPERATOR_CODE
                        FROM  MON.FT_REFILL
                        WHERE
                           REFILL_MEAN<>'SCRATCH' and refill_date in ( select datecode
             from (select distinct datecode from dim.DT_DATES
                         where datecode between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef)
                       minus
                  select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                        where SOURCE_PLATFORM='ZEBRA' and TRANSACTION_TYPE='BONUS_RECHARGE' and TRANSACTION_DATE between trunc(sysdate-refill_number_day_from) and trunc(sysdate-refill_number_day_bef))
                                    )
                 and termination_ind='200'
                 and REFILL_MEAN in ('C2S','SCRATCH')
                        GROUP BY
                        REFILL_DATE
                       ,RECEIVER_PROFILE
                       ,MON.FN_GET_OPERATOR_CODE(RECEIVER_MSISDN)
                       ,(case when refill_mean='SCRATCH' then 'ICC' else 'ZEBRA' end )
                       ,(case when refill_mean='SCRATCH' then 'ICC_TRAFFIC' else 'ZEBRA_TRAFFIC' end )
                       ,(case when refill_mean='SCRATCH' then 'NVX_TOPUP' else 'NVX_C2S' end )
                       ;
                    COMMIT;

    END; -- P_INS_REFILL_TRAFFIC
----------------------------------------------------------------------------------------------------------------
PROCEDURE P_INS_P2P_DATA_TRANSF_REVENUE (p_p2p_number_day_from IN NUMBER DEFAULT 15,p_p2p_number_day_bef IN NUMBER DEFAULT 1)  IS

  p2p_data_number_day_from number:=p_p2p_number_day_from;
  p2p_data_number_day_bef number:=p_p2p_number_day_bef;

  BEGIN
    /* ===>Source FT_A_zte_adjustment_data
                .Revenu des frais de data
     */
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
SELECT   EVENT_DATE TRANSACTION_DATE
   ,SENDER_OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
   ,'P2P_DATA_TRANSFER' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'-' TRANSACTION_SIGN
   ,'ZTE' SOURCE_PLATFORM
   , 'FT_A_DATA_TRANSFER' SOURCE_DATA
   , 'P2P_DATA_TRANSFER' SERVED_SERVICE
   , 'NVX_P2P_DATA' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   , 'HIT' MEASUREMENT_UNIT
   , SUM (1) RATED_COUNT
   , SUM (1) RATED_VOLUME
   , SUM (MONTANT_FRAIS) TAXED_AMOUNT
   , SUM (MONTANT_FRAIS-MONTANT_FRAIS*0.1925) UNTAXED_AMOUNT
   , SYSDATE INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , SENDER_OPERATOR_CODE OPERATOR_CODE
    FROM   MON.FT_A_DATA_TRANSFER       --MON.FT_A_ZTE_ADJUSTMENT_DATA
    WHERE
       -- EVENT_DATE  = '10/05/2018'
        EVENT_DATE  in ( select datecode
from (select distinct datecode from dim.DT_DATES
     where datecode between trunc(sysdate-p2p_data_number_day_from) and trunc(sysdate-p2p_data_number_day_bef)
   minus
select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
    where TRANSACTION_TYPE='P2P_DATA_TRANSFER' and TRANSACTION_DATE between trunc(sysdate-p2p_data_number_day_from) and trunc(sysdate-p2p_data_number_day_bef))
  )
    GROUP BY
    EVENT_DATE
   ,SENDER_OFFER_PROFILE_CODE
   ,SENDER_OPERATOR_CODE ;

      COMMIT;
END; -- P_INS_P2P_DATA_TRANSFERT_REVE
------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE P_INS_OM_DATA_REVENUE (p_subs_number_day_from IN NUMBER DEFAULT 15,p_subs_number_day_bef IN NUMBER DEFAULT 1)  IS

  subs_number_day_from number:=p_subs_number_day_from;
  subs_number_day_bef number:=p_subs_number_day_bef;


  BEGIN
       /* ===>Source Subscription
                .Rrevenu des souscriptions
      */

        -- modifie par dimitri.happi@orange.com le 18/02/2016
        INSERT INTO FT_GLOBAL_ACTIVITY_DAILY
        SELECT DATECODE TRANSACTION_DATE
           ,UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
           ,'OM_DATA' TRANSACTION_TYPE
           ,'MAIN' SUB_ACCOUNT
           ,'+' TRANSACTION_SIGN
           , 'ZTE' SOURCE_PLATFORM
           ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
           , 'IN_TRAFFIC' SERVED_SERVICE
           , 'NVX_OM_DATA' SERVICE_CODE
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
                SUBS_CHANNEL,
                SUBS_SERVICE,
                OPERATOR_CODE
            FROM FT_A_SUBSCRIPTION
            WHERE TRANSACTION_DATE IN
                    ( select datecode
                      from (select distinct datecode from dim.DT_DATES
                      where datecode between trunc(sysdate-subs_number_day_from) and trunc(sysdate-subs_number_day_bef)
                      minus
                      select distinct TRANSACTION_DATE datecode from FT_GLOBAL_ACTIVITY_DAILY
                      where SOURCE_DATA='FT_A_SUBSCRIPTION' and transaction_type ='OM_DATA'  and TRANSACTION_DATE between trunc(sysdate-subs_number_day_from) and trunc(sysdate-subs_number_day_bef))
                     )    and subs_channel='32'
        )
        WHERE
            TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
             AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
        GROUP BY
             DATECODE
            ,UPPER(PRICE_PLAN_NAME)
            ,'OM_DATA'
            ,'MAIN'
            ,'+'
            , 'ZTE'
            ,'FT_A_SUBSCRIPTION'
            , 'IN_TRAFFIC'
            , 'NVX_OM_DATA'
            , 'DEST_ND'
            , NULL
            ,'HIT'
            ,'REVENUE'
            , OPERATOR_CODE;

            COMMIT;

    END; -- P_INS_OM_DATA_REVENUE
----------------------------------------------------------------------------------------------------------------
PROCEDURE P_INS_VAS_DATA_REVENUE (P_VAS_DATA_NUMBER_DAY_FROM IN NUMBER DEFAULT 15,P_VAS_DATA_NUMBER_DAY_BEF IN NUMBER DEFAULT 1)  IS

  p2p_vas_number_day_from number:=P_VAS_DATA_NUMBER_DAY_FROM;
  p2p_vas_number_day_bef number:=P_VAS_DATA_NUMBER_DAY_BEF;



INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
   UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
   ,'OM_DATA' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   , 'ZTE' SOURCE_PLATFORM
   ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
   , 'IN_TRAFFIC' SERVED_SERVICE
   , 'NVX_OM_DATA' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   ,'HIT' MEASUREMENT_UNIT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , NULL LOCATION_CI
   , DATECODE TRANSACTION_DATE
FROM
(
    SELECT  TRANSACTION_DATE AS DATECODE,
        SUBS_BENEFIT_NAME AS BENEFIT_NAME,
        COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
        SUBS_EVENT_RATED_COUNT,
        SUBS_AMOUNT AS TOTAL_AMOUNT,
        SUBS_CHANNEL,
        SUBS_SERVICE,
        OPERATOR_CODE
    FROM AGG.SPARK_FT_A_SUBSCRIPTION
    WHERE TRANSACTION_DATE ='2020-04-15' and subs_channel='32'
)T
WHERE
    TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
     AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
GROUP BY
     DATECODE
    ,UPPER(PRICE_PLAN_NAME)
    ,'OM_DATA'
    ,'MAIN'
    ,'+'
    , 'ZTE'
    ,'FT_A_SUBSCRIPTION'
    , 'IN_TRAFFIC'
    , 'NVX_OM_DATA'
    , 'DEST_ND'
    , NULL
    ,'HIT'
    ,'REVENUE'
    , OPERATOR_CODE;
