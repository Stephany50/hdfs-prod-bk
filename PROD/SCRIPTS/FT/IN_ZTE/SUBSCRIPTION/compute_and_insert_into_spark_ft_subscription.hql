INSERT INTO TABLE MON.SPARK_FT_SUBSCRIPTION PARTITION(TRANSACTION_DATE)
SELECT T_RESULT.*
FROM
    (SELECT
         DATE_FORMAT(NQ_CREATEDDATE, 'HHmmss') TRANSACTION_TIME
       , SUBSTRING(ACC_NBR, -9) SERVED_PARTY_MSISDN
       , (CASE PREPAY_FLAG
           WHEN 1 THEN 'PREPAID'
           WHEN 2 THEN 'POSTPAID'
           WHEN 3 THEN 'HYBRID'
           ELSE CAST(PREPAY_FLAG AS STRING)
         END ) CONTRACT_TYPE
       , NVL(PROD_SPEC.PROD_SPEC_NAME,  CAST(ITSUBSC.PROD_SPEC_CODE AS STRING)) COMMERCIAL_OFFER
       , (CASE PROVIDER_ID
           WHEN 0 THEN 'OCM'
           WHEN 1 THEN 'SET'
           ELSE CAST(PROVIDER_ID AS STRING)
         END) OPERATOR_CODE
       , NVL(CHANSUBSC.CHANNEL_NAME, CAST(ITSUBSC.CHANNEL_ID AS STRING))  SUBSCRIPTION_CHANNEL
       , SERVICE_LIST
        , SERVSUBSC.SUBSCRIPTION_SERVICE_NAME  SUBSCRIPTION_SERVICE
       , NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING))  SUBSCRIPTION_SERVICE_DETAILS
       , NVL(REL_PROD.PROD_SPEC_NAME, ITSUBSC.RELATED_PROD_CODE)  SUBSCRIPTION_RELATED_SERVICE
       , MAX(CASE
            WHEN  Services_dynamique.MSISDN is not null  THEN NVL(cast(Services_dynamique.BDLE_COST as int),0)
            WHEN  NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING))= services_default.BDLE_NAME  and  Services_dynamique.MSISDN  is null   and  ITSUBSC.CHANNEL_ID='32' THEN  NVL(cast (services_default.BDLE_COST as int),0)
            WHEN ITSUBSC.CHANNEL_ID='32' and  Services_dynamique.BDLE_NAME  is null and services_default.BDLE_NAME is null  THEN NVL(AMOUNT_VIA_OM_VAS,0)
            ELSE EVENT_COST / 100
        END )    RATED_AMOUNT
       , 'NULL' MAIN_BALANCE_USED
       , MAX (TO_DATE(ACTIVE_DATE))   ACTIVE_DATE
       , MAX (DATE_FORMAT(ACTIVE_DATE, 'HHmmss')) ACTIVE_TIME
       , MAX (TO_DATE(EXPIRED_DATE)) EXPIRE_DATE
       , MAX (DATE_FORMAT(EXPIRED_DATE, 'HHmmss')) EXPIRE_TIME
       , MAX (NEW_SUBS_STATE)  SUBSCRIPTION_STATUS
       , MIN (NVL (OLD_PROD_SPEC.PROD_SPEC_NAME,  CAST(ITSUBSC.OLD_PROD_SPEC_CODE  AS STRING)))  PREVIOUS_COMMERCIAL_OFFER
       , MAX (OLD_SUBS_STATE)  PREVIOUS_STATUS
       , MAX (NVL(OLD_PRICE_PLAN.PRICE_PLAN_NAME,  CAST(ITSUBSC.OLD_PRICE_PLAN_CODE AS STRING)))   PREVIOUS_SUBS_SERVICE_DETAILS
       , MAX (ITSUBSC.OLD_RELATED_PROD_CODE)  PREVIOUS_SUBS_RELATED_SERVICE
       , 'NULL' TERMINATION_INDICATOR
       , MAX (IF(BENEFIT_BALANCE_LIST = '', NULL,BENEFIT_BALANCE_LIST)) BENEFIT_BALANCE_LIST
       , MAX (IF(BENEFIT_UNIT_LIST = '', NULL,BENEFIT_UNIT_LIST)) BENEFIT_UNIT_LIST
       , MAX (IF(BENEFIT_ADDED_VALUE_LIST = '', NULL,BENEFIT_ADDED_VALUE_LIST)) BENEFIT_ADDED_VALUE_LIST
       , MAX (IF(BENEFIT_RESULT_VALUE_LIST = '', NULL,BENEFIT_RESULT_VALUE_LIST)) BENEFIT_RESULT_VALUE_LIST
       , MAX (IF(BENEFIT_ACTIVE_DATE_LIST = '', NULL,BENEFIT_ACTIVE_DATE_LIST)) BENEFIT_ACTIVE_DATE_LIST
       , MAX (IF(BENEFIT_EXPIRE_DATE_LIST = '', NULL,BENEFIT_EXPIRE_DATE_LIST)) BENEFIT_EXPIRE_DATE_LIST
       , SUM(1) TOTAL_OCCURENCE
       , CURRENT_TIMESTAMP()   INSERT_DATE
       , MAX(ITSUBSC.ORIGINAL_FILE_DATE)   SOURCE_INSERT_DATE
       , MAX(ITSUBSC.ORIGINAL_FILE_NAME)   ORIGINAL_FILE_NAME
       , MIN (NVL(DTSVS.SERVICE_CODE, 'NVX_OTHERS')) SERVICE_CODE
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(VOIX_ONNET, 0))  VOIX_ONNET
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(VOIX_OFFNET, 0)) VOIX_OFFNET
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(VOIX_INTER, 0))  VOIX_INTER
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(VOIX_ROAMING, 0))  VOIX_ROAMING
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(SMS_ONNET, 0)) SMS_ONNET
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(SMS_OFFNET, 0)) SMS_OFFNET
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(SMS_INTER, 0))   SMS_INTER
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(SMS_ROAMING, 0)) SMS_ROAMING
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(DATA_BUNDLE, 0)) DATA_BUNDLE
       , MAX(CASE WHEN ITSUBSC.CHANNEL_ID='32' THEN NVL(AMOUNT_VIA_OM_VAS,0) ELSE EVENT_COST / 100 END * NVL(SVA, 0))  SVA
       , CREATEDDATE TRANSACTION_DATE
       --, BENEFIT_BAL_LIST
       --, ID
     FROM
     (
       SELECT
           ACC_NBR,
           CHANNEL_ID,
           SUBS_EVENT_ID,
           CREATEDDATE,
           NQ_CREATEDDATE,
           OLD_SUBS_STATE,
           NEW_SUBS_STATE,
           EVENT_COST,
           BENEFIT_NAME,
           OLD_PROD_SPEC_CODE,
           PROD_SPEC_CODE,
           OLD_PRICE_PLAN_CODE,
           PRICE_PLAN_CODE,
           OLD_RELATED_PROD_CODE,
           RELATED_PROD_CODE,
           ACTIVE_DATE,
           EXPIRED_DATE,
           PROVIDER_ID,
           PREPAY_FLAG,
           ID,
           BENEFIT_BAL_LIST,
           ORIGINAL_FILE_DATE,
           ORIGINAL_FILE_NAME,
           CONCAT_WS('|', COLLECT_LIST(
                 (CASE ACCT_RES_RATING_UNIT
                   WHEN 'QM' THEN BEN_ACCT_ADD_VAL/100
                   ELSE BEN_ACCT_ADD_VAL
                 END))) BENEFIT_ADDED_VALUE_LIST,
           CONCAT_WS('|', COLLECT_LIST(
                 (CASE ACCT_RES_RATING_UNIT
                   WHEN 'QM' THEN BEN_ACCT_ADD_RESULT/100
                   ELSE BEN_ACCT_ADD_RESULT
                 END))) BENEFIT_RESULT_VALUE_LIST,
           CONCAT_WS('|', COLLECT_LIST(BEN_ACCT_ADD_ACT_DATE)) BENEFIT_ACTIVE_DATE_LIST,
           CONCAT_WS('|', COLLECT_LIST(BEN_ACCT_ADD_EXP_DATE)) BENEFIT_EXPIRE_DATE_LIST,
           CONCAT_WS('|', COLLECT_LIST(NVL(ACCT_RES_NAME, BEN_ACCT_ID))) BENEFIT_BALANCE_LIST,
           CONCAT_WS('|', COLLECT_LIST(NVL(ACCT_RES_RATING_UNIT, BEN_ACCT_ID))) BENEFIT_UNIT_LIST,
           CONCAT_WS('|', COLLECT_SET(NVL(ACCT_RES_RATING_SERVICE_CODE, BEN_ACCT_ID))) SERVICE_LIST
       FROM
       (
         SELECT
           ACC_NBR,
           CHANNEL_ID,
           SUBS_EVENT_ID,
           CREATEDDATE,
           NQ_CREATEDDATE,
           OLD_SUBS_STATE,
           NEW_SUBS_STATE,
           CAST(EVENT_COST AS DOUBLE) EVENT_COST,
           BENEFIT_NAME,
           OLD_PROD_SPEC_CODE,
           PROD_SPEC_CODE,
           OLD_PRICE_PLAN_CODE,
           PRICE_PLAN_CODE,
           OLD_RELATED_PROD_CODE,
           RELATED_PROD_CODE,
           ACTIVE_DATE,
           EXPIRED_DATE,
           PROVIDER_ID,
           PREPAY_FLAG,
           BENEFIT_BAL_LIST,
           ORIGINAL_FILE_DATE,
           ORIGINAL_FILE_NAME,
           SPLIT(BEN_BAL, '&')[0] BEN_ACCT_ID,
           CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[1] AS INT)) AS STRING) BEN_ACCT_ADD_VAL,
           CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[2] AS INT)) AS STRING) BEN_ACCT_ADD_RESULT,
           SPLIT(BEN_BAL, '&')[3] BEN_ACCT_ADD_ACT_DATE,
           SPLIT(BEN_BAL, '&')[4] BEN_ACCT_ADD_EXP_DATE,
           ID
         FROM
         (
             SELECT A.*, ROW_NUMBER() OVER(ORDER BY CREATEDDATE) ID
             FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION A
             WHERE A.CREATEDDATE = '###SLICE_VALUE###' AND original_file_name not like '%in_postpaid%'
         ) A
         LATERAL VIEW EXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '#')) TMP AS BEN_BAL
       ) ITSUBSC
       LEFT JOIN (SELECT ACCT_RES_STD_CODE, MAX(ACCT_RES_NAME) ACCT_RES_NAME, MAX(ACCT_RES_RATING_SERVICE_CODE) ACCT_RES_RATING_SERVICE_CODE, MAX(ACCT_RES_RATING_UNIT) ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM GROUP BY ACCT_RES_STD_CODE) BALANCE_TYPE_ITEM ON ITSUBSC.BEN_ACCT_ID = BALANCE_TYPE_ITEM.ACCT_RES_STD_CODE
       GROUP BY
           ACC_NBR,
           CHANNEL_ID,
           SUBS_EVENT_ID,
           CREATEDDATE,
           NQ_CREATEDDATE,
           OLD_SUBS_STATE,
           NEW_SUBS_STATE,
           EVENT_COST,
           BENEFIT_NAME,
           OLD_PROD_SPEC_CODE,
           PROD_SPEC_CODE,
           OLD_PRICE_PLAN_CODE,
           PRICE_PLAN_CODE,
           OLD_RELATED_PROD_CODE,
           RELATED_PROD_CODE,
           ACTIVE_DATE,
           EXPIRED_DATE,
           PROVIDER_ID,
           PREPAY_FLAG,
           ORIGINAL_FILE_DATE,
           ORIGINAL_FILE_NAME,
           BENEFIT_BAL_LIST,
           ID
     ) ITSUBSC
     LEFT JOIN DIM.DT_SUBSCRIPTION_SERVICE SERVSUBSC ON NVL(ITSUBSC.SUBS_EVENT_ID, 1000000) = SERVSUBSC.SUBSCRIPTION_SERVICE_ID
     LEFT JOIN DIM.DT_SUBSCRIPTION_CHANNEL CHANSUBSC ON NVL(ITSUBSC.CHANNEL_ID, 1000000) = CHANSUBSC.CHANNEL_ID
     LEFT JOIN (SELECT NVL(PRICE_PLAN_CODE, 'UNKNOWN')PRICE_PLAN_CODE , MIN( PRICE_PLAN_NAME) PRICE_PLAN_NAME
                FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT
                WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
                GROUP BY NVL(PRICE_PLAN_CODE, 'UNKNOWN')) PRICE_PLAN
         ON NVL(ITSUBSC.PRICE_PLAN_CODE, '1000000') = PRICE_PLAN.PRICE_PLAN_CODE
     LEFT JOIN (SELECT NVL(PRICE_PLAN_CODE, 'UNKNOWN')PRICE_PLAN_CODE, MIN(PRICE_PLAN_NAME) PRICE_PLAN_NAME
                FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT
                WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
                GROUP BY NVL(PRICE_PLAN_CODE, 'UNKNOWN')) OLD_PRICE_PLAN
         ON NVL(ITSUBSC.OLD_PRICE_PLAN_CODE, '1000000') = OLD_PRICE_PLAN.PRICE_PLAN_CODE
     LEFT JOIN (SELECT STD_CODE, MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
                FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
                WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
                GROUP BY STD_CODE) PROD_SPEC
         ON NVL(ITSUBSC.PROD_SPEC_CODE, '1000000') =  PROD_SPEC.STD_CODE
     LEFT JOIN (SELECT STD_CODE, MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
                FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
                WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
                GROUP BY STD_CODE) REL_PROD
         ON NVL(ITSUBSC.RELATED_PROD_CODE, '1000000') = REL_PROD.STD_CODE
     LEFT JOIN (SELECT STD_CODE, MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
                FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
                WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
                GROUP BY STD_CODE) OLD_PROD_SPEC
         ON NVL(ITSUBSC.OLD_PROD_SPEC_CODE, '1000000') =  OLD_PROD_SPEC.STD_CODE
     LEFT JOIN (SELECT EVENT, MAX(SERVICE_CODE) SERVICE_CODE, MIN(VOIX_ONNET) VOIX_ONNET, MIN(VOIX_OFFNET) VOIX_OFFNET, MIN(VOIX_INTER)VOIX_INTER, MIN(VOIX_ROAMING)VOIX_ROAMING,
                    MIN(SMS_ONNET) SMS_ONNET, MIN(SMS_OFFNET) SMS_OFFNET, MIN(SMS_INTER) SMS_INTER, MIN(SMS_ROAMING)SMS_ROAMING, MIN(DATA_BUNDLE) DATA_BUNDLE, MIN(SVA) SVA, MIN(PRIX) PRIX
                FROM DIM.DT_SERVICES DTSVS GROUP BY EVENT ) DTSVS ON NVL(PRICE_PLAN.PRICE_PLAN_NAME, SERVSUBSC.SUBSCRIPTION_SERVICE_NAME) = DTSVS.EVENT
     LEFT JOIN (SELECT MAX(IPP_AMOUNT) AMOUNT_VIA_OM_VAS, IPP_NAME
                FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT IPP_EXTRACT
                WHERE IPP_EXTRACT.ORIGINAL_FILE_DATE IN (SELECT MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT)
                GROUP BY IPP_NAME) IPP_EXTRACT ON NVL(PRICE_PLAN.PRICE_PLAN_NAME, SERVSUBSC.SUBSCRIPTION_SERVICE_NAME) = IPP_EXTRACT.IPP_NAME
     LEFT JOIN (select msisdn,bdle_name,bdle_cost from cdr.dt_Services_dynamique) Services_dynamique on Services_dynamique.bdle_name = NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING)) and  SUBSTRING(ITSUBSC.ACC_NBR, -9)  = Services_dynamique.MSISDN and NVL(CHANSUBSC.CHANNEL_NAME, CAST(ITSUBSC.CHANNEL_ID AS STRING))='32'
     LEFT JOIN (select * from cdr.dt_services_default) services_default on services_default.bdle_name = NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING)) and  NVL(CHANSUBSC.CHANNEL_NAME, CAST(ITSUBSC.CHANNEL_ID AS STRING))='32'

     GROUP BY CREATEDDATE
     , ITSUBSC.NQ_CREATEDDATE
     , SUBSTRING(ACC_NBR, -9)
     , (CASE PREPAY_FLAG
         WHEN 1 THEN 'PREPAID'
         WHEN 2 THEN 'POSTPAID'
         WHEN 3 THEN 'HYBRID'
         ELSE CAST(PREPAY_FLAG AS STRING)
       END )
     , NVL(PROD_SPEC.PROD_SPEC_NAME,  CAST(ITSUBSC.PROD_SPEC_CODE AS STRING))
     , (CASE PROVIDER_ID
         WHEN 0 THEN 'OCM'
         WHEN 1 THEN 'SET'
         ELSE CAST(PROVIDER_ID AS STRING)
       END)
     , NVL(CHANSUBSC.CHANNEL_NAME, CAST(ITSUBSC.CHANNEL_ID AS STRING))
     , ITSUBSC.SERVICE_LIST
     , SERVSUBSC.SUBSCRIPTION_SERVICE_NAME
     , NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING))
     , NVL(REL_PROD.PROD_SPEC_NAME, ITSUBSC.RELATED_PROD_CODE)
     , CURRENT_TIMESTAMP()
     , ITSUBSC.BENEFIT_BAL_LIST
) T_RESULT

