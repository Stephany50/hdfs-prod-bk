add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
create temporary function GET_NNP_MSISDN_9DIGITS as 'cm.orange.bigdata.udf.GetNnpMsisdn9Digits';
--create temporary function GET_OPERATOR_CODE as 'cm.orange.bigdata.udf.GetOperatorCode';

INSERT INTO REPORT.REVENUE_SUMMARY_DAILY  (EVENT_DATE, IN_NBR_ADJ_RBT, IN_ADJ_RBT_TAX_AMT, IN_NBR_ADJ_USS, IN_ADJ_USS_TAX_AMT
                                           ,IN_NBR_ADJ_VOI_SMS, IN_ADJ_VOI_SMS_TAX_AMT, IN_NBR_ADJ_VEXT, IN_ADJ_VEXT_TAX_AMT
                                           ,IN_NBR_ADJ_PAR, IN_ADJ_PAR_TAX_AMT, IN_NBR_ADJ_FBO, IN_ADJ_FBO_TAX_AMT, IN_NBR_ADJ_CEL
                                           ,IN_ADJ_CEL_TAX_AMT, IN_NBR_ADJ_SIG, IN_ADJ_SIG_TAX_AMT )
SELECT 
    EVENT_DATE
    , SUM(IF(SERVICE_CODE = 'NVX_RBT', RATED_COUNT, 0)) NBR_ADJ_RBT
    , SUM(IF(SERVICE_CODE = 'NVX_RBT', TAXED_AMOUNT, 0)) ADJ_RBT_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_USS', RATED_COUNT, 0)) NBR_ADJ_USS
    , SUM(IF(SERVICE_CODE = 'NVX_USS', TAXED_AMOUNT, 0)) ADJ_USS_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'VOI_SMS', RATED_COUNT, 0)) NBR_ADJ_VOI_SMS
    , SUM(IF(SERVICE_CODE = 'VOI_SMS', TAXED_AMOUNT, 0)) ADJ_VOI_SMS_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_VEXT', RATED_COUNT, 0)) NBR_ADJ_VEXT
    , SUM(IF(SERVICE_CODE = 'NVX_VEXT', TAXED_AMOUNT, 0)) ADJ_VEXT_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_PAR', RATED_COUNT, 0)) NBR_ADJ_PAR
    , SUM(IF(SERVICE_CODE = 'NVX_PAR', TAXED_AMOUNT, 0)) ADJ_PAR_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_FBO', RATED_COUNT, 0)) NBR_ADJ_FBO
    , SUM(IF(SERVICE_CODE = 'NVX_FBO', TAXED_AMOUNT, 0)) ADJ_FBO_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_CEL', RATED_COUNT, 0)) NBR_ADJ_CEL
    , SUM(IF(SERVICE_CODE = 'NVX_CEL', TAXED_AMOUNT, 0)) ADJ_CEL_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_SIG', RATED_COUNT, 0)) NBR_ADJ_SIG
    , SUM(IF(SERVICE_CODE = 'NVX_SIG', TAXED_AMOUNT, 0)) ADJ_SIG_TAX_AMT
FROM 
    (SELECT     
        CREATE_DATE EVENT_DATE
        , C.PROFILE COMMERCIAL_OFFER_CODE
        , B.GLOBAL_USAGE_CODE SERVICE_CODE
        , SUM (IF(CHARGE > 0, 1, 0)) RATED_COUNT
        , SUM (IF(CHARGE > 0, CAST(CHARGE AS DOUBLE)/100, 0)) TAXED_AMOUNT
        , C.OPERATOR_CODE OPERATOR_CODE
    FROM CDR.IT_ZTE_ADJUSTMENT A
    LEFT JOIN (SELECT USAGE_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
    LEFT JOIN (SELECT ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE)  OPERATOR_CODE
               FROM MON.FT_CONTRACT_SNAPSHOT A
               LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.FT_CONTRACT_SNAPSHOT
                          WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                          GROUP BY ACCESS_KEY) B 
                ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
               WHERE B.ACCESS_KEY IS NOT NULL                 
               GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE ) C
    ON C.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
    WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37')                      
    GROUP BY  
        CREATE_DATE
        , C.PROFILE
        , B.GLOBAL_USAGE_CODE
        , OPERATOR_CODE
    )A 
LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM'
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE
;

