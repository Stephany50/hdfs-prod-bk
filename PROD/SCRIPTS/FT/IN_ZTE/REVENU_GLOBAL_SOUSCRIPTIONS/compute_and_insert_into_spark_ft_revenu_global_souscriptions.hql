INSERT INTO MON.SPARK_FT_REVENU_GLOBAL_SOUSCRIPTIONS 
SELECT
   'BUN_VOX' SERVICE_CODE
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , TRANSACTION_DATE
FROM
(
    SELECT TRANSACTION_DATE
        , BENEFIT_NAME
        , PRICE_PLAN_NAME
        , ((COEFF_ONNET + COEFF_OFFNET + COEFF_INTER + COEFF_ROAMING_VOIX)/100)*SUBS_EVENT_RATED_COUNT SUBS_EVENT_RATED_COUNT
        , ((COEFF_ONNET + COEFF_OFFNET + COEFF_INTER + COEFF_ROAMING_VOIX)/100)*TOTAL_AMOUNT TOTAL_AMOUNT
        , SUBS_CHANNEL
        , SUBS_SERVICE
        , OPERATOR_CODE
    FROM 
    (
        SELECT  TRANSACTION_DATE,
            SUBS_BENEFIT_NAME AS BENEFIT_NAME,
            COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
            SUBS_EVENT_RATED_COUNT,
            SUBS_AMOUNT AS TOTAL_AMOUNT,
            SUBS_CHANNEL,
            SUBS_SERVICE,
            OPERATOR_CODE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION
        WHERE 
            TRANSACTION_DATE = '###SLICE_VALUE###'
    ) X0
    LEFT JOIN 
    DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1
    ON UPPER(TRIM(X0.BENEFIT_NAME)) = UPPER(TRIM(X1.BDLE_NAME))
)T
WHERE TOTAL_AMOUNT>0 
GROUP BY
    TRANSACTION_DATE

UNION

SELECT
   'BUN_DATA' SERVICE_CODE
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , TRANSACTION_DATE
FROM
(
    SELECT TRANSACTION_DATE
        , BENEFIT_NAME
        , PRICE_PLAN_NAME
        , ((COEFF_DATA + COEFF_ROAMING_DATA)/100)*SUBS_EVENT_RATED_COUNT SUBS_EVENT_RATED_COUNT
        , ((COEFF_DATA + COEFF_ROAMING_DATA)/100)*TOTAL_AMOUNT TOTAL_AMOUNT
        , SUBS_CHANNEL
        , SUBS_SERVICE
        , OPERATOR_CODE
    FROM 
    (
        SELECT  TRANSACTION_DATE,
            SUBS_BENEFIT_NAME AS BENEFIT_NAME,
            COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
            SUBS_EVENT_RATED_COUNT,
            SUBS_AMOUNT AS TOTAL_AMOUNT,
            SUBS_CHANNEL,
            SUBS_SERVICE,
            OPERATOR_CODE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION
        WHERE 
            TRANSACTION_DATE = '###SLICE_VALUE###'
    ) X0
    LEFT JOIN 
    DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1
    ON UPPER(TRIM(X0.BENEFIT_NAME)) = UPPER(TRIM(X1.BDLE_NAME))
)T
WHERE TOTAL_AMOUNT>0 
GROUP BY
    TRANSACTION_DATE

UNION

SELECT
   'BUN_SMS' SERVICE_CODE
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , TRANSACTION_DATE
FROM
(
    SELECT TRANSACTION_DATE
        -- , BENEFIT_NAME
        , PRICE_PLAN_NAME
        , ((COEF_SMS + COEFF_ROAMING_SMS)/100)*SUBS_EVENT_RATED_COUNT SUBS_EVENT_RATED_COUNT
        , ((COEF_SMS + COEFF_ROAMING_SMS)/100)*TOTAL_AMOUNT TOTAL_AMOUNT
        , SUBS_CHANNEL
        , SUBS_SERVICE
        , OPERATOR_CODE
    FROM 
    (
        SELECT  TRANSACTION_DATE,
            SUBS_BENEFIT_NAME AS BENEFIT_NAME,
            COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
            SUBS_EVENT_RATED_COUNT,
            SUBS_AMOUNT AS TOTAL_AMOUNT,
            SUBS_CHANNEL,
            SUBS_SERVICE,
            OPERATOR_CODE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION
        WHERE 
            TRANSACTION_DATE = '###SLICE_VALUE###'
    ) X0
    LEFT JOIN 
    DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1
    ON UPPER(TRIM(X0.BENEFIT_NAME)) = UPPER(TRIM(X1.BDLE_NAME))
)T
WHERE TOTAL_AMOUNT>0 
GROUP BY
    TRANSACTION_DATE

UNION

SELECT
    'NVX_VAS_DATA' SERVICE_CODE
     , SUM ( ( (COEFF_DATA + COEFF_ROAMING_DATA)/100) * MAIN_AMOUNT) TAXED_AMOUNT
     , SUM ((1-0.1925) * ( (COEFF_DATA + COEFF_ROAMING_DATA)/100) * MAIN_AMOUNT) UNTAXED_AMOUNT
     , TRANSACTION_DATE
FROM 
(
    SELECT * 
    FROM MON.SPARK_FT_SUBS_RETAIL_ZEBRA 
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
        AND MAIN_AMOUNT > 0
) X0 
LEFT JOIN 
DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1 
ON UPPER(TRIM(X0.SUBSCRIPTION_SERVICE_DETAILS)) = UPPER(TRIM(X1.BDLE_NAME))
GROUP BY
    TRANSACTION_DATE

UNION

SELECT
    'NVX_VAS_VOICE' SERVICE_CODE
     , SUM ( ((COEFF_ONNET + COEFF_OFFNET + COEFF_INTER + COEFF_ROAMING_VOIX)/100) * MAIN_AMOUNT) TAXED_AMOUNT
     , SUM ((1-0.1925) * ((COEFF_ONNET + COEFF_OFFNET + COEFF_INTER + COEFF_ROAMING_VOIX)/100) * MAIN_AMOUNT) UNTAXED_AMOUNT
     , TRANSACTION_DATE TRANSACTION_DATE
FROM 
(
    SELECT * 
    FROM MON.SPARK_FT_SUBS_RETAIL_ZEBRA 
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND MAIN_AMOUNT > 0
) X0 
LEFT JOIN 
DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1 
ON UPPER(TRIM(X0.SUBSCRIPTION_SERVICE_DETAILS)) = UPPER(TRIM(X1.BDLE_NAME))
GROUP BY
    TRANSACTION_DATE