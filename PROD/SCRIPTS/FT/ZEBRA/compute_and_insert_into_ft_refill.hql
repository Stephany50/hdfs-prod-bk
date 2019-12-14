INSERT INTO MON.FT_REFILL PARTITION(REFILL_DATE)
SELECT
    ZEBRA.TRANSFER_ID         REFILL_ID                
    , DATE_FORMAT(ZEBRA.TRANSFER_DATE_TIME, 'HHmmss')  REFILL_TIME       
    , ZEBRA.RECEIVER_MSISDN     RECEIVER_MSISDN 
    , OFFER.OFFER_NAME RECEIVER_PROFILE
    , NULL                      RECEIVER_IMSI     
    , ZEBRA.SENDER_MSISDN       SENDER_MSISDN  
    , NULL                      SENDER_PROFILE
    , ZEBRA.CHANNEL_TYPE        REFILL_MEAN       
    , ZEBRA.TRANSACTION_TYPE    REFILL_TYPE
    , (CASE WHEN NVL(RECEIVER_CREDIT_AMOUNT, 0) = 0 THEN ZEBRA.MAIN_BUNDLE_VAL 
            ELSE (NVL(RECEIVER_CREDIT_AMOUNT,0))/100
        END) REFILL_AMOUNT
    , ZEBRA.BONUS_BUNDLE_VAL REFILL_BONUS  
    , ZEBRA.TRANSFER_STATUS     TERMINATION_IND   
    , NULL                      REFILL_CODE
    , NULL                      REFILL_DESCRIPTION
    , GET_OPERATOR_CODE(RECEIVER_MSISDN) RECEIVER_OPERATOR_CODE
    , GET_OPERATOR_CODE(SENDER_MSISDN) SENDER_OPERATOR_CODE
    , ZEBRA.SENDER_CATEGORY
    , ZEBRA.RECEIVER_CATEGORY
    , CAST(ZEBRA.SEND_PRE_BAL AS DOUBLE)/100  SENDER_PRE_BAL
    , CAST(ZEBRA.SEND_POST_BAL AS DOUBLE)/100 SENDER_POST_BAL
    , CAST(ZEBRA.RCVR_PRE_BAL AS DOUBLE)/100  RECEIVER_PRE_BAL 
    , CAST(ZEBRA.RCVR_POST_BAL AS DOUBLE)/100 RECEIVER_POST_BAL
    , MIN(ZEBRA.INSERT_DATE)    ENTRY_DATE   
    , MIN(ZEBRA.ORIGINAL_FILE_NAME)      ORIGINAL_FILE_NAME
    , CURRENT_TIMESTAMP INSERT_DATE
    , ZEBRA.TRANSFER_DATE       REFILL_DATE
FROM (
  SELECT MAX(TRANSFER_ID) TRANSFER_ID
     ,MAX(TRANSFER_DATE_TIME) TRANSFER_DATE_TIME
     ,MAX(RECEIVER_MSISDN) RECEIVER_MSISDN
     ,MAX(SENDER_MSISDN) SENDER_MSISDN
     ,MAX(CHANNEL_TYPE) CHANNEL_TYPE
     ,MAX(TRANSACTION_TYPE) TRANSACTION_TYPE
     ,MAX(RECEIVER_CREDIT_AMOUNT) RECEIVER_CREDIT_AMOUNT
     ,MAX(TRANSFER_STATUS) TRANSFER_STATUS
     ,MAX(INSERT_DATE) INSERT_DATE
     ,MAX(ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME
     ,MAX(SENDER_CATEGORY) SENDER_CATEGORY
     ,MAX(RECEIVER_CATEGORY) RECEIVER_CATEGORY
     ,MAX(SEND_PRE_BAL) SEND_PRE_BAL
     ,MAX(SEND_POST_BAL) SEND_POST_BAL
     ,MAX(RCVR_PRE_BAL) RCVR_PRE_BAL
     ,MAX(RCVR_POST_BAL) RCVR_POST_BAL
     ,MAX(SERVICE_CLASS_CODE) SERVICE_CLASS_CODE
     ,MAX(TRANSFER_DATE) TRANSFER_DATE
     ,SUM(ZEBRA.MAIN_BUNDLE_VAL) MAIN_BUNDLE_VAL
     ,SUM(ZEBRA.BONUS_BUNDLE_VAL) BONUS_BUNDLE_VAL
     ,ID
  FROM(
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
      FROM (
        SELECT A.*, ROW_NUMBER() OVER(ORDER BY TRANSFER_DATE) ID
        FROM CDR.IT_ZEBRA_TRANSAC A
        WHERE A.TRANSFER_DATE = '###SLICE_VALUE###'
      ) A
     LATERAL VIEW EXPLODE(SPLIT(NVL(BONUS_ACCOUNT_DETAILS, ''), '[|]')) TMP AS BUNDLE_DET
  )ZEBRA
  GROUP BY ID  
) ZEBRA
LEFT JOIN (SELECT SUBSTR(TRIM(MAX(PROFILE_NAME)), 1, 25) OFFER_NAME,
            STD_CODE,
            MAX(ORIGINAL_FILE_DATE)
           FROM CDR.SPARK_IT_ZTE_PROFILE A
           LEFT JOIN (SELECT STD_CODE,MAX(ORIGINAL_FILE_DATE) MAX_DATE FROM CDR.SPARK_IT_ZTE_PROFILE
                      WHERE ORIGINAL_FILE_DATE <= '###SLICE_VALUE###'
                      GROUP BY STD_CODE) B 
            ON B.STD_CODE = A.STD_CODE AND B.MAX_DATE = A.ORIGINAL_FILE_DATE
            WHERE B.STD_CODE IS NOT NULL                 
            GROUP BY A.STD_CODE) OFFER
ON IF (CAST(NVL(ZEBRA.SERVICE_CLASS_CODE, 'UNKNOW') AS INT) IS NOT NULL,-1,CAST(ZEBRA.SERVICE_CLASS_CODE AS INT))=OFFER.STD_CODE
GROUP BY
    ZEBRA.TRANSFER_ID               
    , DATE_FORMAT(ZEBRA.TRANSFER_DATE_TIME, 'HHmmss')    
    , ZEBRA.RECEIVER_MSISDN 
    , OFFER.OFFER_NAME 
    , ZEBRA.SENDER_MSISDN
    , ZEBRA.CHANNEL_TYPE       
    , ZEBRA.TRANSACTION_TYPE
    , (CASE WHEN NVL(RECEIVER_CREDIT_AMOUNT, 0) = 0 THEN ZEBRA.MAIN_BUNDLE_VAL 
            ELSE (NVL(RECEIVER_CREDIT_AMOUNT,0))/100
        END)
    , ZEBRA.BONUS_BUNDLE_VAL  
    , ZEBRA.TRANSFER_STATUS
    , GET_OPERATOR_CODE(RECEIVER_MSISDN)
    , GET_OPERATOR_CODE(SENDER_MSISDN)
    , ZEBRA.SENDER_CATEGORY
    , ZEBRA.RECEIVER_CATEGORY
    , CAST(ZEBRA.SEND_PRE_BAL AS DOUBLE)/100
    , CAST(ZEBRA.SEND_POST_BAL AS DOUBLE)/100
    , CAST(ZEBRA.RCVR_PRE_BAL AS DOUBLE)/100 
    , CAST(ZEBRA.RCVR_POST_BAL AS DOUBLE)/100
    , ZEBRA.TRANSFER_DATE

UNION
SELECT     
    NULL REFILL_ID           
    , TRANSACTION_TIME REFILL_TIME
    , SERVED_PARTY_MSISDN RECEIVER_MSISDN
    , COMMERCIAL_OFFER RECEIVER_PROFILE
    , NULL RECEIVER_IMSI
    , NULL SENDER_MSISDN 
    , NULL SENDER_PROFILE
    , 'SCRATCH' REFILL_MEAN     
    , 'REFILL' REFILL_TYPE
    , NVL(REFILL_AMOUNT,0) REFILL_AMOUNT
    , (CASE WHEN BENEFIT_BALANCE_LIST = 'Promo' THEN CAST(BENEFIT_BAL_ADDED_VALUE_LIST AS DOUBLE)
            WHEN BENEFIT_BALANCE_LIST LIKE 'Promo%' THEN CAST(SUBSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL), 1, INSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL),'|')-1) AS DOUBLE)
            WHEN BENEFIT_BALANCE_LIST = 'SMS|Promo' THEN CAST(SUBSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL), 1, INSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL),'|')-1) AS DOUBLE) 
            ELSE 0 END)  REFILL_BONUS
    , '200' TERMINATION_IND
    , NULL REFILL_CODE
    , NULL REFILL_DESCRIPTION
    , OPERATOR_CODE               RECEIVER_OPERATOR_CODE
    , NULL SENDER_OPERATOR_CODE
    , NULL SENDER_CATEGORY
    , NULL RECEIVER_CATEGORY
    , NULL SENDER_PRE_BAL
    , NULL SENDER_POST_BAL
    , MAX(NVL(REFILL_BALANCE_RESULT_VALUE, 0) - NVL(REFILL_AMOUNT,0)) RECEIVER_PRE_BAL
    , MAX(NVL(REFILL_BALANCE_RESULT_VALUE, 0)) RECEIVER_POST_BAL
    , MAX(A.SOURCE_INSERT_DATE) ENTRY_DATE
    , MAX(A.ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME   
    , CURRENT_TIMESTAMP INSERT_DATE
    , TRANSACTION_DATE REFILL_DATE
FROM MON.FT_RECHARGE A
WHERE REFILL_PAYMENT_METHOD = 'SCRATCH CARD' AND A.TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY
    TRANSACTION_DATE 
    , TRANSACTION_TIME               
    , SERVED_PARTY_MSISDN                   
    , COMMERCIAL_OFFER               
    , NVL(REFILL_AMOUNT,0) 
    , (CASE WHEN BENEFIT_BALANCE_LIST = 'Promo' THEN CAST(BENEFIT_BAL_ADDED_VALUE_LIST AS DOUBLE)
            WHEN BENEFIT_BALANCE_LIST LIKE 'Promo%' THEN CAST(SUBSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL), 1, INSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL),'|')-1) AS DOUBLE)
            WHEN BENEFIT_BALANCE_LIST = 'SMS|Promo' THEN CAST(SUBSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL), 1, INSTR(NVL(BENEFIT_BAL_ADDED_VALUE_LIST,NULL),'|')-1) AS DOUBLE) 
            ELSE 0 END) 
    , OPERATOR_CODE      

;
