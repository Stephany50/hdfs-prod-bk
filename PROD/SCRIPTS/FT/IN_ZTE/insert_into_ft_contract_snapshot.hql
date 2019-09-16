INSERT INTO MON.FT_CONTRACT_SNAPSHOT PARTITION(EVENT_DATE)
SELECT
    NULL CONTRACT_ID
    ,NULL CUSTOMER_ID
    ,ZTE.ACC_NBR ACCESS_KEY
    ,ZTE.ACCT_ID ACCOUNT_ID
    ,TO_DATE(ZTE.COMPLETED_DATE) ACTIVATION_DATE
    ,TO_DATE(ZTE.DEACTIVATION_DATE) DEACTIVATION_DATE
    ,NULL INACTIVITY_BEGIN_DATE
    ,NULL BLOCKED
    ,NULL EXHAUSTED
    ,NULL PERIODIC_FEE
    ,NULL SCRATCH_RELOAD_SUSP
    ,TO_DATE(ZTE.UPDATE_DATE) COMMERCIAL_OFFER_ASSIGN_DATE
    ,ZTE.PRICE_PLAN_NAME COMMERCIAL_OFFER
    ,NULL CURRENT_STATUS
    ,NULL STATUS_DATE
    ,NULL LOGIN
    ,ZTE.DEF_LANG_ID LANG
    ,NULL LOCATION
    ,ZTE.IMSI MAIN_IMSI
    ,NULL MSID_TYPE
    ,ZTE.PRICE_PLAN_NAME PROFILE
    ,NULL BAD_RELOAD_ATTEMPTS
    ,TO_DATE(ZTE.ACCT_UPDATE_DATE) LAST_TOPUP_DATE
    ,TO_DATE(ZTE.ACCT_UPDATE_DATE) LAST_CREDIT_UPDATE_DATE
    ,NULL BAD_PIN_ATTEMPTS
    ,NULL BAD_PWD_ATTEMPTS
    ,(CASE WHEN UPPER(PRICE_PLAN_NAME) LIKE 'POSTPAID%' THEN 'POSTPAID' WHEN UPPER(PRICE_PLAN_NAME) LIKE 'PREPAID%' THEN 'PREPAID' ELSE 'PREPAID' END) OSP_ACCOUNT_TYPE
    ,NULL INACTIVITY_CREDIT_LOSS
    ,NULL DEALER_ID
    ,TO_DATE(ZTE.ACCT_CREATED_DATE) PROVISIONING_DATE
    ,(ZTE.GROSS_MAIN_BALANCE + ZTE.CONSUME_MAIN_BALANCE + ZTE.RESERVE_MAIN_BALANCE) MAIN_CREDIT
    ,(ZTE.GROSS_PROMO_BALANCE + ZTE.CONSUME_PROMO_BALANCE + ZTE.RESERVE_PROMO_BALANCE) PROMO_CREDIT
    ,NULL SMS_CREDIT
    ,NULL DATA_CREDIT
    ,NULL USED_CREDIT_MONTH
    ,NULL USED_CREDIT_LIFE
    ,NULL BUNDLE_LIST
    ,NULL BUNDLE_UNIT_LIST
    ,NULL PROMO_AND_DISCOUNT_LIST
    ,CURRENT_TIMESTAMP() INSERT_DATE
    ,(CASE WHEN UPPER(PRICE_PLAN_NAME) LIKE 'POSTPAID%' THEN 'FT_BSCS_CONTRACT' WHEN UPPER(PRICE_PLAN_NAME) LIKE 'PREPAID%' THEN 'IT_ICC_ACCOUNT' ELSE 'IT_ICC_ACCOUNT' END)  SRC_TABLE
    ,(CASE WHEN (ZTE.PROD_PROD_STATE='DEACTIVATED' AND ZTE.BLOCK_REASON='20000000000000') THEN 'INACTIVE' ELSE ZTE.PROD_PROD_STATE END ) OSP_STATUS
    ,NULL BSCS_COMM_OFFER_ID
    ,NULL BSCS_COMM_OFFER
    ,NULL INITIAL_SELECTION_DONE
    ,NULL NOMORE_CREDIT
    ,NULL PWD_BLOCKED
    ,NULL FIRST_EVENT_DONE
    ,NULL CUST_EXT_ID
    ,NULL CUST_GROUP
    ,NULL CUST_CATEGORY
    ,NULL CUST_BILLCYCLE
    ,NULL CUST_SEGMENT
    ,UPPER(B.CONTRACT_TYPE) OSP_CONTRACT_TYPE
    ,NULL OSP_CUST_COMMERCIAL_OFFER
    ,NULL OSP_CUSTOMER_CGLIST
    ,NULL OSP_CUSTOMER_FORMULE
    ,NULL BSCS_ACTIVATION_DATE
    ,NULL BSCS_DEACTIVATION_DATE
    ,ZTE.OPERATOR_CODE OPERATOR_CODE
    ,ZTE.BALANCE_LIST
    ,PREVIOUS_STATUS
    ,ZTE.CURRENT_STATUS CURRENT_STATUS_1
    ,PROD_PROD_STATE_DATE STATE_DATETIME
    ,EVENT_DATE
FROM
(
  SELECT DISTINCT
      FIRST_VALUE(EVENT_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) EVENT_DATE
    , ACC_NBR
    ,FIRST_VALUE(ACC_NBR_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACC_NBR_STATE
    ,FIRST_VALUE(STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) STATE_DATE
    ,FIRST_VALUE(ACC_NBR_TYPE_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACC_NBR_TYPE_ID
    ,FIRST_VALUE(IMSI) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) IMSI
    ,FIRST_VALUE(UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) UPDATE_DATE
    ,FIRST_VALUE(SUBS_CUST_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) SUBS_CUST_ID
    ,FIRST_VALUE(ACCT_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACCT_ID
    ,FIRST_VALUE(PRICE_PLAN_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PRICE_PLAN_ID
    ,FIRST_VALUE(DEF_LANG_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) DEF_LANG_ID
    ,FIRST_VALUE(CUST_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_ID
    ,FIRST_VALUE(CUST_NAME) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_NAME
    ,FIRST_VALUE(ADDRESS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ADDRESS
    ,FIRST_VALUE(CUST_TYPE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_TYPE
    ,FIRST_VALUE(PARENT_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PARENT_ID
    ,FIRST_VALUE(CREATED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CREATED_DATE
    ,FIRST_VALUE(CUST_UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_UPDATE_DATE
    ,FIRST_VALUE(CUST_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_STATE
    ,FIRST_VALUE(CUST_STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_STATE_DATE
    ,FIRST_VALUE(CUST_CODE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_CODE
    ,FIRST_VALUE(PROD_CREATED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_CREATED_DATE
    ,FIRST_VALUE(BLOCK_REASON) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BLOCK_REASON
    ,FIRST_VALUE(COMPLETED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) COMPLETED_DATE
    ,FIRST_VALUE(PROD_PROD_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_PROD_STATE
    ,FIRST_VALUE(PROD_PROD_STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_PROD_STATE_DATE
    ,FIRST_VALUE(PROD_UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_UPDATE_DATE
    ,FIRST_VALUE(PROD_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_STATE
    ,FIRST_VALUE(PROD_STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_STATE_DATE
    ,FIRST_VALUE(PRICE_PLAN_NAME) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PRICE_PLAN_NAME
    ,FIRST_VALUE(MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE
    ,FIRST_VALUE(PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_BALANCE
    ,FIRST_VALUE(MAIN_BALANCE_EFF_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE_EFF_DATE
    ,FIRST_VALUE(MAIN_BALANCE_EXP_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE_EXP_DATE
    ,FIRST_VALUE(PROMO_BALANCE_EFF_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_BALANCE_EFF_DATE
    ,FIRST_VALUE(PROMO_BALANCE_EXP_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_BALANCE_EXP_DATE
    ,FIRST_VALUE(ACCT_UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACCT_UPDATE_DATE
    ,FIRST_VALUE(ACCT_CREATED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACCT_CREATED_DATE
    ,FIRST_VALUE(DEACTIVATION_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) DEACTIVATION_DATE
    ,FIRST_VALUE(GROSS_MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) GROSS_MAIN_BALANCE
    ,FIRST_VALUE(CONSUME_MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CONSUME_MAIN_BALANCE
    ,FIRST_VALUE(GROSS_PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) GROSS_PROMO_BALANCE
    ,FIRST_VALUE(CONSUME_PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CONSUME_PROMO_BALANCE
    ,FIRST_VALUE(OPERATOR_CODE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) OPERATOR_CODE
    ,FIRST_VALUE(BALANCE_LIST) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BALANCE_LIST
    ,FIRST_VALUE(RESERVE_PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) RESERVE_PROMO_BALANCE
    ,FIRST_VALUE(RESERVE_MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) RESERVE_MAIN_BALANCE
    ,FIRST_VALUE(BUNDLE_MONEY_INTER) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BUNDLE_MONEY_INTER
    ,FIRST_VALUE(BUNDLE_OFFNET_MONEY) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BUNDLE_OFFNET_MONEY
    ,FIRST_VALUE(MAIN_BALANCE_REAL) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE_REAL
    ,FIRST_VALUE(PROMO_ONNET_SPECIAL) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_ONNET_SPECIAL
    ,FIRST_VALUE(LOAN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) LOAN_BALANCE
    ,FIRST_VALUE(MAIN_SASSAYE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_SASSAYE
    ,FIRST_VALUE(PROMO_SASSAYE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_SASSAYE
    ,FIRST_VALUE(PROMO) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO
    ,FIRST_VALUE(PROMO_ONNET) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_ONNET
    ,FIRST_VALUE(SET_VOICEONNET) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) SET_VOICEONNET
    ,FIRST_VALUE(SET_PROMO) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) SET_PROMO
    ,FIRST_VALUE(PROD_STATUS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_STATUS
    ,FIRST_VALUE(CUR.CURRENT_STATUS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CURRENT_STATUS
    ,FIRST_VALUE(CASE WHEN CUR.CURRENT_STATUS <> PREV.CURRENT_STATUS  THEN PREV.CURRENT_STATUS ELSE PREV.PREVIOUS_STATUS END) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PREVIOUS_STATUS
  FROM
  (
  SELECT 
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('###SLICE_VALUE###', 'yyyy-MM-dd'))) EVENT_DATE
    -- ACC_NBR Source
    ,ACC_NBR.ACC_NBR
    ,(CASE WHEN ACC_NBR.ACC_NBR_STATE='A' THEN 'NOT USED' WHEN ACC_NBR.ACC_NBR_STATE='C' THEN 'USED' ELSE ACC_NBR.ACC_NBR_STATE END) ACC_NBR_STATE
    ,ACC_NBR.STATE_DATE
    ,(CASE WHEN ACC_NBR.ACC_NBR_TYPE_ID=1 THEN 'MSISDN' WHEN ACC_NBR.ACC_NBR_TYPE_ID=2 THEN 'CUG' ELSE CAST(ACC_NBR.ACC_NBR_TYPE_ID AS STRING) END) ACC_NBR_TYPE_ID
    --Subscriber Source
    ,SUBS.IMSI
    ,SUBS.UPDATE_DATE
    ,SUBS.SUBS_CUST_ID
    ,SUBS.SUBS_ACCT_ID ACCT_ID
    ,SUBS.PRICE_PLAN_ID
    ,(CASE WHEN SUBS.DEF_LANG_ID='1' THEN 'FR' WHEN SUBS.DEF_LANG_ID='2' THEN 'EN' ELSE CAST(SUBS.DEF_LANG_ID AS STRING) END) DEF_LANG_ID
    --Customer Source
    ,CUST.CUST_ID
    ,CUST.CUST_NAME
    ,CUST.ADDRESS
    ,(CASE WHEN CUST.CUST_TYPE='A' THEN 'Particulier' WHEN CUST.CUST_TYPE='B' THEN 'Entreprise' ELSE CUST.CUST_TYPE END) CUST_TYPE
    ,CUST.PARENT_ID
    ,CUST.CREATED_DATE
    ,CUST.UPDATE_DATE CUST_UPDATE_DATE
    ,CUST.STATE CUST_STATE
    ,CUST.STATE_DATE CUST_STATE_DATE
    ,CUST.CUST_CODE
    --Prod source
    ,PROD.CREATED_DATE PROD_CREATED_DATE
    ,PROD.BLOCK_REASON
    ,PROD.COMPLETED_DATE
    ,(CASE WHEN PROD.PROD_STATE='G' THEN 'VALID'
    WHEN PROD.PROD_STATE='A' THEN 'ACTIVE'
    WHEN (PROD.PROD_STATE='D' OR (PROD.PROD_STATE='E' AND PROD.BLOCK_REASON='20000000000000'))THEN 'INACTIVE'
    WHEN (PROD.PROD_STATE='E'AND PROD.BLOCK_REASON<>'20000000000000') THEN 'DEACTIVATED'
    WHEN PROD.PROD_STATE='B' THEN 'TERMINATED'
    ELSE PROD.PROD_STATE END
    ) PROD_PROD_STATE
    ,PROD.PROD_STATE_DATE PROD_PROD_STATE_DATE
    ,PROD.UPDATE_DATE PROD_UPDATE_DATE
    ,PROD.STATE PROD_STATE
    ,PROD.STATE_DATE PROD_STATE_DATE
    --Source Price Plan
    ,UPPER(PRICE_PLAN.PRICE_PLAN_NAME) PRICE_PLAN_NAME
    --Source Balance
    ,BAL.MAIN_BALANCE
    ,BAL.PROMO_BALANCE
    ,BAL.MAIN_BALANCE_EFF_DATE
    ,BAL.MAIN_BALANCE_EXP_DATE
    ,BAL.PROMO_BALANCE_EFF_DATE
    ,BAL.PROMO_BALANCE_EXP_DATE
    --Source Account
    ,ACCT.UPDATE_DATE  ACCT_UPDATE_DATE
    ,ACCT.CREATED_DATE ACCT_CREATED_DATE
    --Mandatory Field
    --Source PROD_ATTR_VAL
    ,DATE_ADD(BAL.MAIN_BALANCE_EXP_DATE, 30) DEACTIVATION_DATE
    ,BAL.GROSS_MAIN_BALANCE GROSS_MAIN_BALANCE
    ,BAL.CONSUME_MAIN_BALANCE CONSUME_MAIN_BALANCE
    ,BAL.GROSS_PROMO_BALANCE GROSS_PROMO_BALANCE
    ,BAL.CONSUME_PROMO_BALANCE CONSUME_PROMO_BALANCE
    -- Ajout des nouvelles colonnes
    ,(CASE WHEN PROD.SP_ID='0' THEN 'OCM' WHEN PROD.SP_ID='1' THEN 'SET' ELSE 'UNKNOWN' END) OPERATOR_CODE
    ,BALANCE_LIST
    ,BAL.RESERVE_PROMO_BALANCE
    ,BAL.RESERVE_MAIN_BALANCE
    ,BAL.BUNDLE_MONEY_INTER
    ,BAL.BUNDLE_OFFNET_MONEY
    ,BAL.MAIN_BALANCE_REAL
    ,BAL.PROMO_ONNET_SPECIAL
    ,BAL.LOAN_BALANCE
    ,BAL.MAIN_SASSAYE
    ,BAL.PROMO_SASSAYE
    ,BAL.PROMO
    ,BAL.PROMO_ONNET
    ,BAL.SET_VOICEONNET
    ,BAL.SET_PROMO
    ,PROD.PROD_STATE AS PROD_STATUS
    ,(CASE WHEN PROD.PROD_STATE='G' THEN 'VALID'
       WHEN PROD.PROD_STATE='A' THEN 'ACTIVE'
       WHEN PROD.PROD_STATE='D'THEN 'INACTIVE'
       WHEN (PROD.PROD_STATE='E' AND SUBSTR(PROD.BLOCK_REASON, 3, 1) <> '0') THEN 'DEACTIVATED'
       WHEN (PROD.PROD_STATE='E'AND SUBSTR(PROD.BLOCK_REASON, 3, 1) = '0' AND SUBSTR(PROD.BLOCK_REASON, 1, 1) <> '0') THEN 'SUSPENDED'
       WHEN PROD.PROD_STATE='B' THEN 'TERMINATED'
      ELSE PROD.PROD_STATE END) AS CURRENT_STATUS
    ,NULL PREVIOUS_STATUS
  FROM ( SELECT ACC_NBR, STATE_DATE, ACC_NBR_STATE, ACC_NBR_TYPE_ID FROM CDR.IT_ZTE_ACC_NBR_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' AND ACC_NBR_STATE = 'C') ACC_NBR
  LEFT JOIN (SELECT ACC_NBR SUBS_ACC_NBR, IMSI, UPDATE_DATE, CUST_ID SUBS_CUST_ID, ACCT_ID SUBS_ACCT_ID, PRICE_PLAN_ID, DEF_LANG_ID, SUBS_ID  FROM CDR.IT_ZTE_SUBS_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') SUBS  ON ACC_NBR.ACC_NBR = SUBS.SUBS_ACC_NBR
  LEFT JOIN (SELECT CUST_ID, CUST_CODE, CUST_NAME, ADDRESS, CUST_TYPE, PARENT_ID, CREATED_DATE, UPDATE_DATE, STATE, STATE_DATE FROM CDR.IT_ZTE_CUST_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') CUST ON SUBS.SUBS_CUST_ID = CUST.CUST_ID
  LEFT JOIN (SELECT PROD_ID, CREATED_DATE, BLOCK_REASON, COMPLETED_DATE, STATE, PROD_STATE, PROD_STATE_DATE, UPDATE_DATE, STATE_DATE, SP_ID FROM CDR.IT_ZTE_PROD_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' AND INDEP_PROD_ID IS NULL AND NVL(ROUTING_ID, '100') IN ('1','2','3') AND PROD_STATE <> 'B') PROD ON SUBS.SUBS_ID = PROD.PROD_ID
  LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE in (select max(original_file_date) from cdr.IT_ZTE_PRICE_PLAN_EXTRACT)) PRICE_PLAN ON SUBS.PRICE_PLAN_ID = PRICE_PLAN.PRICE_PLAN_ID
  LEFT JOIN (SELECT ACCT_ID, UPDATE_DATE, CREATED_DATE FROM CDR.IT_ZTE_ACCT_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') ACCT ON SUBS.SUBS_ACCT_ID = ACCT.ACCT_ID
  LEFT JOIN (
  SELECT ACCT_ID
  ,NULL MAIN_BALANCE
  ,NULL PROMO_BALANCE
  ,MAX(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='MB' THEN EFF_DATE ELSE NULL END) MAIN_BALANCE_EFF_DATE
  ,MAX(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='MB' THEN EXP_DATE ELSE NULL END) MAIN_BALANCE_EXP_DATE
  ,MAX(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='PB' THEN EFF_DATE ELSE NULL END) PROMO_BALANCE_EFF_DATE
  ,MAX(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='PB' THEN EXP_DATE ELSE NULL END) PROMO_BALANCE_EXP_DATE
  ,SUM(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='MB' THEN -NVL(GROSS_BAL, 0)/100 ELSE 0 END) GROSS_MAIN_BALANCE
  ,SUM(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='PB' THEN -NVL(GROSS_BAL, 0)/100 ELSE 0 END) GROSS_PROMO_BALANCE
  ,SUM(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='MB' THEN -NVL(CONSUME_BAL, 0)/100 ELSE 0 END) CONSUME_MAIN_BALANCE
  ,SUM(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='PB' THEN -NVL(CONSUME_BAL, 0)/100 ELSE 0 END) CONSUME_PROMO_BALANCE
  ,SUM(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='MB' THEN -NVL(RESERVE_BAL, 0)/100 ELSE 0 END) RESERVE_MAIN_BALANCE
  ,SUM(CASE WHEN DT_BAL.ACCT_RES_RATING_TYPE='PB' THEN -NVL(RESERVE_BAL, 0)/100 ELSE 0 END) RESERVE_PROMO_BALANCE
  ,CONCAT_WS('|', COLLECT_LIST(CONCAT_WS('\;', CAST(BAL.ACCT_RES_ID AS STRING), CAST(EFF_DATE AS STRING), CAST(UPDATE_DATE AS STRING), CAST(EXP_DATE AS STRING), CAST(CAST(-NVL(GROSS_BAL, 0) AS BIGINT) AS STRING), CAST(CAST(-NVL(CONSUME_BAL, 0) AS BIGINT) AS STRING), CAST(CAST(-NVL(RESERVE_BAL, 0) AS BIGINT) AS STRING)))) BALANCE_LIST
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 30 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) BUNDLE_MONEY_INTER
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 45 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) BUNDLE_OFFNET_MONEY
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 1 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) MAIN_BALANCE_REAL
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 27 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) PROMO_ONNET_SPECIAL
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 20 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) LOAN_BALANCE
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 21 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) MAIN_SASSAYE
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 22 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) PROMO_SASSAYE
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 2 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) PROMO
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 3 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) PROMO_ONNET
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 15 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) SET_VOICEONNET
  ,SUM(CASE WHEN BAL.ACCT_RES_ID = 16 THEN -NVL(GROSS_BAL, 0)/100-NVL(CONSUME_BAL, 0)/100-NVL(RESERVE_BAL, 0)/100 ELSE 0 END) SET_PROMO
  FROM CDR.IT_ZTE_BAL_EXTRACT BAL
  LEFT JOIN (SELECT DISTINCT ACCT_RES_ID, ACCT_RES_RATING_TYPE FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_RES_RATING_UNIT='QM') DT_BAL ON BAL.ACCT_RES_ID = DT_BAL.ACCT_RES_ID
  WHERE BAL.ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
  GROUP BY ACCT_ID
  ) BAL ON SUBS.SUBS_ACCT_ID = BAL.ACCT_ID
  ) CUR
  LEFT JOIN (SELECT DISTINCT ACCESS_KEY, STATE_DATETIME, CURRENT_STATUS_1 CURRENT_STATUS, PREVIOUS_STATUS FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = DATE_SUB(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('###SLICE_VALUE###', 'yyyy-MM-dd'))), 1)) PREV ON (CUR.ACC_NBR = PREV.ACCESS_KEY AND CUR.PROD_CREATED_DATE = PREV.STATE_DATETIME)
) ZTE
LEFT JOIN DIM.DT_OFFER_PROFILES B ON ZTE.PRICE_PLAN_NAME = B.PROFILE_CODE
WHERE ZTE.COMPLETED_DATE IS NOT NULL;