INSERT INTO MON.SPARK_FT_RECHARGE PARTITION(TRANSACTION_DATE)
SELECT
    DATE_FORMAT(PAY_TIME, 'HHmmss') TRANSACTION_TIME
    , GET_NNP_MSISDN_9DIGITS(ACC_NBR) SERVED_PARTY_MSISDN
    , (CASE PREPAY_FLAG
          WHEN 1 THEN 'PREPAID'
          WHEN 2 THEN 'POSTPAID'
          WHEN 3 THEN 'HYBRID'
          ELSE CAST(PREPAY_FLAG AS STRING)
      END ) CONTRACT_TYPE
    , NVL(ACCTSNAPSHOT.PROFILE, 'UNKNOWN')
    , (CASE PROVIDER_ID
          WHEN 0 THEN 'OCM'
          WHEN 1 THEN 'SET'
          ELSE CAST(PROVIDER_ID AS STRING)
      END ) OPERATOR_CODE
    , NVL(CHANRECH.CHANNEL_NAME, CAST (ITRECH.CHANNEL_ID AS STRING )) REFILL_CHANNEL
    , (CASE PAYMENT_METHOD 
        WHEN '1' THEN 'CASH'
        WHEN '2' THEN 'CREDIT/DEBIT CARD'
        WHEN '3' THEN 'CHECK'
        WHEN '4' THEN 'SCRATCH CARD'
        WHEN '5' THEN 'OTHERS'
        ELSE CAST(PAYMENT_METHOD AS STRING) 
     END) REFILL_PAYMENT_METHOD
    , MAX(IF(NVL(BALTYPE.ACCT_RES_RATING_TYPE,CAST(ITRECH.ACCT_RES_CODE AS STRING)) = 'MB','YES','NO')) MAIN_BALANCE_USED
    , NVL(BALTYPE.ACCT_RES_NAME,CAST(ITRECH.ACCT_RES_CODE AS STRING)) REFILL_BALANCE
    , MAX(-CAST(RESULT_BALANCE AS DOUBLE)/100) REFILL_BALANCE_RESULT_VALUE
    , MAX(DATEDIFF(TO_DATE(NEW_EXP_DATE),PAY_DATE)) REFILL_BALANCE_VALIDITY_DAYS
    , MAX(TO_DATE(OLD_EXP_DATE))  REFILL_BALANCE_PREV_EXPIREDATE
    , MAX(TO_DATE(NEW_EXP_DATE))  REFILL_BALANCE_NEW_EXPIREDATE
    , BENEFIT_NAME REFILL_BONUS_SERVICE
    , MAX(-CAST(BILL_AMOUNT AS DOUBLE)/100) REFILL_AMOUNT
    , MAX(CAST(LOAN_AMOUNT AS DOUBLE)/100) LOAN_AMOUNT
    , MAX(CAST(COMMISSION_AMOUNT AS DOUBLE)/100) COMMISSION_AMOUNT
    , MAX (IF(BENEFIT_PRICEPLAN_LIST = '', NULL,BENEFIT_PRICEPLAN_LIST)) BENEFIT_PRICEPLAN_LIST
    , MAX (IF(BENEFIT_PP_ACTIVE_DATE_LIST = '', NULL,BENEFIT_PP_ACTIVE_DATE_LIST)) BENEFIT_PP_ACTIVE_DATE_LIST
    , MAX (IF(BENEFIT_PP_EXPIRE_DATE_LIST = '', NULL,BENEFIT_PP_EXPIRE_DATE_LIST)) BENEFIT_PP_EXPIRE_DATE_LIST
    , MAX (IF(BENEFIT_BALANCE_LIST = '', NULL,BENEFIT_BALANCE_LIST)) BENEFIT_BALANCE_LIST
    , MAX (IF(BENEFIT_BAL_UNIT_LIST = '', NULL,BENEFIT_BAL_UNIT_LIST)) BENEFIT_BAL_UNIT_LIST
    , MAX (IF(BENEFIT_BAL_ADDED_VALUE_LIST = '', NULL,BENEFIT_BAL_ADDED_VALUE_LIST)) BENEFIT_BAL_ADDED_VALUE_LIST
    , MAX (IF(BENEFIT_BAL_RESULT_VALUE_LIST = '', NULL,BENEFIT_BAL_RESULT_VALUE_LIST)) BENEFIT_BAL_RESULT_VALUE_LIST
    , MAX (IF(BENEFIT_BAL_ACTIVE_DATE_LIST = '', NULL,BENEFIT_BAL_ACTIVE_DATE_LIST)) BENEFIT_BAL_ACTIVE_DATE_LIST
    , MAX (IF(BENEFIT_BAL_EXPIRE_DATE_LIST = '', NULL,BENEFIT_BAL_EXPIRE_DATE_LIST)) BENEFIT_BAL_EXPIRE_DATE_LIST
    , SUM(1) TOTAL_OCCURENCE
    , CURRENT_TIMESTAMP INSERT_DATE
    , MAX(ITRECH.ORIGINAL_FILE_DATE) SOURCE_INSERT_DATE
    , MAX(ITRECH.ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME
    , PAY_DATE TRANSACTION_DATE
FROM(
    SELECT
        MAX(PAYMENT_ID) PAYMENT_ID
        ,MAX(ACCT_CODE) ACCT_CODE
        ,MAX(ACC_NBR) ACC_NBR
        ,MAX(PAY_TIME) PAY_TIME
        ,MAX(ACCT_RES_CODE) ACCT_RES_CODE
        ,MAX(BILL_AMOUNT) BILL_AMOUNT
        ,MAX(RESULT_BALANCE) RESULT_BALANCE
        ,MAX(CHANNEL_ID) CHANNEL_ID
        ,MAX(PAYMENT_METHOD) PAYMENT_METHOD
        ,MAX(DAYS) DAYS
        ,MAX(OLD_EXP_DATE) OLD_EXP_DATE
        ,MAX(NEW_EXP_DATE) NEW_EXP_DATE
        ,MAX(BENEFIT_NAME) BENEFIT_NAME
        ,MAX(BENEFIT_BAL_LIST) BENEFIT_BAL_LIST
        ,MAX(BENEFIT_PRICEPLAN) BENEFIT_PRICEPLAN
        ,MAX(TRANSACTIONSN) TRANSACTIONSN
        ,MAX(PROVIDER_ID) PROVIDER_ID
        ,MAX(PREPAY_FLAG) PREPAY_FLAG
        ,MAX(LOAN_AMOUNT) LOAN_AMOUNT
        ,MAX(COMMISSION_AMOUNT) COMMISSION_AMOUNT
        ,MAX(ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME
        ,MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE
        ,CONCAT_WS('|', COLLECT_LIST(BEN_PP_ACT_DATE)) BENEFIT_PP_ACTIVE_DATE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(BEN_PP_EXP_DATE)) BENEFIT_PP_EXPIRE_DATE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(NVL(PP.PRICE_PLAN_NAME, BEN_PP_ID))) BENEFIT_PRICEPLAN_LIST
        ,CONCAT_WS('|', COLLECT_LIST(
                (CASE ACCT_RES_RATING_UNIT
                  WHEN 'QM' THEN BEN_ACCT_ADD_VAL/100
                  ELSE BEN_ACCT_ADD_VAL
                END))) BENEFIT_BAL_ADDED_VALUE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(
                (CASE ACCT_RES_RATING_UNIT
                  WHEN 'QM' THEN BEN_ACCT_ADD_RESULT/100
                  ELSE BEN_ACCT_ADD_RESULT
                END))) BENEFIT_BAL_RESULT_VALUE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(BEN_ACCT_ADD_ACT_DATE)) BENEFIT_BAL_ACTIVE_DATE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(BEN_ACCT_ADD_EXP_DATE)) BENEFIT_BAL_EXPIRE_DATE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(NVL(BALTYPE.ACCT_RES_NAME, BEN_ACCT_ID))) BENEFIT_BALANCE_LIST
        ,CONCAT_WS('|', COLLECT_LIST(NVL(BALTYPE.ACCT_RES_RATING_UNIT, BEN_ACCT_ID))) BENEFIT_BAL_UNIT_LIST
        ,MAX(PAY_DATE) PAY_DATE
        ,ID
    FROM(
        SELECT
            PAYMENT_ID
            ,ACCT_CODE
            ,ACC_NBR
            ,PAY_TIME
            ,ACCT_RES_CODE
            ,BILL_AMOUNT
            ,RESULT_BALANCE
            ,CHANNEL_ID
            ,PAYMENT_METHOD
            ,DAYS
            ,OLD_EXP_DATE
            ,NEW_EXP_DATE
            ,BENEFIT_NAME
            ,BENEFIT_BAL_LIST
            ,BENEFIT_PRICEPLAN
            ,TRANSACTIONSN
            ,PROVIDER_ID
            ,PREPAY_FLAG
            ,LOAN_AMOUNT
            ,COMMISSION_AMOUNT
            ,ORIGINAL_FILE_NAME
            ,ORIGINAL_FILE_DATE
            ,IF(BEN_PP<>'',SPLIT(BEN_PP, '&')[0],NULL) BEN_PP_ID
            ,IF(BEN_PP<>'',SPLIT(BEN_PP, '&')[1],NULL) BEN_PP_ACT_DATE
            ,IF(BEN_PP<>'',SPLIT(BEN_PP, '&')[2],NULL) BEN_PP_EXP_DATE
            ,SPLIT(BEN_BAL, '&')[0] BEN_ACCT_ID
            ,CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[1] AS INT)) AS STRING) BEN_ACCT_ADD_VAL
            ,CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[2] AS INT)) AS STRING) BEN_ACCT_ADD_RESULT
            ,SPLIT(BEN_BAL, '&')[3] BEN_ACCT_ADD_ACT_DATE
            ,SPLIT(BEN_BAL, '&')[4] BEN_ACCT_ADD_EXP_DATE
            ,PAY_DATE
            ,ID
        FROM(
                SELECT A.*, ROW_NUMBER() OVER(ORDER BY PAY_DATE) ID
                FROM CDR.SPARK_IT_ZTE_RECHARGE A
                WHERE A.PAY_DATE = '2020-01-10'
            ) A
            LATERAL VIEW POSEXPLODE(SPLIT(NVL(BENEFIT_PRICEPLAN, ''), '#')) TMP1 AS IND_PP, BEN_PP
            LATERAL VIEW POSEXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '#')) TMP2 AS IND_BAL, BEN_BAL
        WHERE IND_PP = IND_BAL
        ) ITRECH
        LEFT JOIN (SELECT PRICE_PLAN_CODE ,
                    MIN( PRICE_PLAN_NAME) PRICE_PLAN_NAME
                    FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT
                    WHERE ORIGINAL_FILE_DATE = '2020-01-10' AND PRICE_PLAN_CODE <> "TestPasFact" 
                    GROUP BY PRICE_PLAN_CODE)PP
        ON ITRECH.BEN_PP_ID = PP.PRICE_PLAN_CODE
        LEFT JOIN (SELECT ACCT_RES_STD_CODE, 
                    MIN(ACCT_RES_NAME) ACCT_RES_NAME, 
                    MIN(ACCT_RES_RATING_TYPE) ACCT_RES_RATING_TYPE,
                    MAX(ACCT_RES_RATING_UNIT) ACCT_RES_RATING_UNIT
                    FROM DIM.DT_BALANCE_TYPE_ITEM GROUP BY ACCT_RES_STD_CODE
                    ) BALTYPE 
        ON ITRECH.BEN_ACCT_ID = BALTYPE.ACCT_RES_STD_CODE
        GROUP BY ID
    ) ITRECH
LEFT JOIN DIM.DT_SUBSCRIPTION_CHANNEL CHANRECH ON CHANRECH.CHANNEL_ID = NVL(ITRECH.CHANNEL_ID, 1000000)
LEFT JOIN (SELECT ACCT_RES_STD_CODE, 
            MIN(ACCT_RES_NAME) ACCT_RES_NAME, 
            MIN(ACCT_RES_RATING_TYPE) ACCT_RES_RATING_TYPE
           FROM DIM.DT_BALANCE_TYPE_ITEM GROUP BY ACCT_RES_STD_CODE
            ) BALTYPE 
ON BALTYPE.ACCT_RES_STD_CODE = NVL(ITRECH.ACCT_RES_CODE, '1000000')
LEFT JOIN (SELECT A.ACCESS_KEY,
            EVENT_DATE,
            MIN(UPPER (NVL (PROFILE, SUBSTR(NVL(BSCS_COMM_OFFER,COMMERCIAL_OFFER), INSTR(NVL(BSCS_COMM_OFFER,COMMERCIAL_OFFER),'|')+1)))) PROFILE 
           FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
           LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                      WHERE EVENT_DATE between date_sub('2020-01-10',7) AND '2020-01-10'
                      GROUP BY ACCESS_KEY) B 
            ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
            WHERE B.ACCESS_KEY IS NOT NULL                 
            GROUP BY A.ACCESS_KEY, EVENT_DATE
            ) ACCTSNAPSHOT
ON ACCTSNAPSHOT.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(ITRECH.ACC_NBR)
WHERE  PAY_DATE = '2020-01-10'
GROUP BY DATE_FORMAT(PAY_TIME, 'HHmmss')
    ,GET_NNP_MSISDN_9DIGITS(ACC_NBR)
    ,(CASE PREPAY_FLAG
          WHEN 1 THEN 'PREPAID'
          WHEN 2 THEN 'POSTPAID'
          WHEN 3 THEN 'HYBRID'
          ELSE CAST(PREPAY_FLAG AS STRING)
      END )
    , NVL(ACCTSNAPSHOT.PROFILE, 'UNKNOWN')
    ,(CASE PROVIDER_ID
          WHEN 0 THEN 'OCM'
          WHEN 1 THEN 'SET'
          ELSE CAST(PROVIDER_ID AS STRING)
      END )
    ,NVL(BALTYPE.ACCT_RES_NAME,CAST(ITRECH.ACCT_RES_CODE AS STRING))
    ,NVL(CHANRECH.CHANNEL_NAME, CAST (ITRECH.CHANNEL_ID AS STRING ))
    ,(CASE PAYMENT_METHOD 
        WHEN '1' THEN 'CASH'
        WHEN '2' THEN 'CREDIT/DEBIT CARD'
        WHEN '3' THEN 'CHECK'
        WHEN '4' THEN 'SCRATCH CARD'
        WHEN '5' THEN 'OTHERS'
        ELSE CAST(PAYMENT_METHOD AS STRING) 
     END)
    , BENEFIT_NAME
    , PAY_DATE
 
 

