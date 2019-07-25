INSERT INTO AGG.REVENUE_SUMMARY_DAILY (EVENT_DATE, IN_NBR_GPRS_SVA_POST, IN_GPRS_SVA_POST_VOL, IN_GPRS_SVA_POST_TAX_AMT
                                           , IN_NBR_GPRS_PAYGO_POST, IN_GPRS_PAYGO_POST_VOL, IN_GPRS_PAYGO_POST_TAX_AMT)
SELECT
    EVENT_DATE
    , SUM(NBR_GPRS_SVA_POST) NBR_GPRS_SVA_POST
    , SUM(GPRS_SVA_POST_VOL) GPRS_SVA_POST_VOL
    , SUM(GPRS_SVA_POST_TAX_AMT) GPRS_SVA_POST_TAX_AMT
    , SUM(NBR_GPRS_PAYGO_POST) NBR_GPRS_PAYGO_POST
    , SUM(GPRS_PAYGO_POST_VOL) GPRS_PAYGO_POST_VOL
    , SUM(GPRS_PAYGO_POST_TAX_AMT)GPRS_PAYGO_POST_TAX_AMT
FROM (
SELECT
    EVENT_DATE
    , SUM(IF(SERVICE_CODE = 'NVX_GPRS_SVA', RATED_COUNT, 0)) NBR_GPRS_SVA_POST
    , SUM(IF(SERVICE_CODE = 'NVX_GPRS_SVA',RATED_VOLUME, 0)) GPRS_SVA_POST_VOL
    , SUM(IF(SERVICE_CODE = 'NVX_GPRS_SVA',TAXED_AMOUNT, 0)) GPRS_SVA_POST_TAX_AMT
    , SUM(IF(SERVICE_CODE = 'NVX_GPRS_PAYGO', RATED_COUNT, 0)) NBR_GPRS_PAYGO_POST
    , SUM(IF(SERVICE_CODE = 'NVX_GPRS_PAYGO',RATED_VOLUME, 0)) GPRS_PAYGO_POST_VOL
    , SUM(IF(SERVICE_CODE = 'NVX_GPRS_PAYGO',TAXED_AMOUNT, 0))GPRS_PAYGO_POST_TAX_AMT
FROM 
    (SELECT     
        DATECODE EVENT_DATE
        , UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
        , IF(SERVICE_NAME IS NOT NULL , 'NVX_GPRS_SVA', 'NVX_GPRS_PAYGO') SERVICE_CODE
        , SUM (1) RATED_COUNT
        , SUM (TOTAL_UNIT) RATED_VOLUME
        , SUM (MAIN_COST) TAXED_AMOUNT
        , OPERATOR_CODE OPERATOR_CODE
    FROM MON.FT_A_GPRS_ACTIVITY_POST
    WHERE DATECODE = '###SLICE_VALUE###' AND MAIN_COST > 0                       
    GROUP BY  
        DATECODE
        , UPPER(COMMERCIAL_OFFER)
        , IF(SERVICE_NAME IS NOT NULL , 'NVX_GPRS_SVA', 'NVX_GPRS_PAYGO')
        , OPERATOR_CODE
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

