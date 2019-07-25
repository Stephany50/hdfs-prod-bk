INSERT INTO AGG.REVENUE_SUMMARY_DAILY  (EVENT_DATE, IN_NBR_GSM_VOX_POST, IN_GSM_VOX_POST_VOL, IN_GSM_VOX_POST_TAX_AMT, IN_NBR_GSM_SMS_POST, IN_GSM_SMS_POST_TAX_AMT)
SELECT
    EVENT_DATE
    , SUM(NBR_GSM_VOX_POST) NBR_GSM_VOX_POST
    , SUM(GSM_VOX_POST_VOL) GSM_VOX_POST_VOL
    , SUM(GSM_VOX_POST_TAX_AMT) GSM_VOX_POST_TAX_AMT
    , SUM(NBR_GSM_SMS_POST) NBR_GSM_SMS_POST
    , SUM(GSM_SMS_POST_TAX_AMT) GSM_SMS_POST_TAX_AMT
FROM (
SELECT
    EVENT_DATE
    , SUM(IF(SERVICE_CODE = 'VOI_VOX', RATED_COUNT, 0)) NBR_GSM_VOX_POST
    , SUM(IF(SERVICE_CODE = 'VOI_VOX',RATED_VOLUME, 0)) GSM_VOX_POST_VOL
    , SUM(IF(SERVICE_CODE = 'VOI_VOX',TAXED_AMOUNT, 0)) GSM_VOX_POST_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_SMS', RATED_COUNT, 0)) NBR_GSM_SMS_POST
    , SUM(IF(SERVICE_CODE = 'NVX_SMS',TAXED_AMOUNT, 0)) GSM_SMS_POST_TAX_AMT
FROM
    (SELECT
        TRANSACTION_DATE EVENT_DATE
        ,OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
        ,(case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end) SERVICE_CODE
        , SUM (TOTAL_COUNT) RATED_COUNT
        , SUM ( CASE SERVICE_CODE
                 WHEN 'VOI_VOX' THEN DURATION
                 WHEN 'NVX_SMS' THEN TOTAL_COUNT
                 WHEN 'NVX_USS' THEN TOTAL_COUNT
                ELSE TOTAL_COUNT END) RATED_VOLUME
        , SUM (MAIN_RATED_AMOUNT) TAXED_AMOUNT
        , OPERATOR_CODE OPERATOR_CODE
    FROM AGG.FT_GSM_TRAFFIC_REVENUE_POST
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'  AND NVL(MAIN_RATED_AMOUNT,0)>0
    GROUP BY
        TRANSACTION_DATE
        ,OFFER_PROFILE_CODE
        ,(case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end)
        ,OPERATOR_CODE
    )A
    LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM'
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE
) T
GROUP BY
    EVENT_DATE
;

