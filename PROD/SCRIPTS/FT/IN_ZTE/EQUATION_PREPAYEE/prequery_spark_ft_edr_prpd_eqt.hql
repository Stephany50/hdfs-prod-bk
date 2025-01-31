SELECT IF(VOICE_EXIST>0 and DATA_EXIST>0 and ADJUSTMENT_EXIST>0 and RECHARGE_EXIST>0 and SUBSCRIPTION_EXIST>0
    and TRANSFER_EXIST>0 and ED_EXIST>0 and LOAN_CDR_EXIST>0 and EC_EXIST>0 and (DELEXPBAL_EXIST>0 or '###SLICE_VALUE###'='2021-11-17' )and IF(BALRESET_EXIST >0, TRUE, IF(MISSING_BALRESET=0, TRUE, FALSE)) and RECURRING_EXIST>0 and PRICE_PLAN_EXIST>0  ,"OK","NOK") TABLES_EXIST
FROM (SELECT COUNT(*) VOICE_EXIST FROM CDR.SPARK_IT_ZTE_VOICE_SMS WHERE START_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) DATA_EXIST FROM CDR.SPARK_IT_ZTE_DATA WHERE START_DATE='###SLICE_VALUE###') B,
(SELECT COUNT(*) ADJUSTMENT_EXIST FROM CDR.SPARK_IT_ZTE_ADJUSTMENT WHERE CREATE_DATE='###SLICE_VALUE###') C,
(SELECT COUNT(*) RECHARGE_EXIST FROM CDR.SPARK_IT_ZTE_RECHARGE WHERE PAY_DATE='###SLICE_VALUE###') D,
(SELECT COUNT(*) SUBSCRIPTION_EXIST FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE='###SLICE_VALUE###') E,
(SELECT COUNT(*) TRANSFER_EXIST FROM CDR.SPARK_IT_ZTE_TRANSFER WHERE PAY_DATE='###SLICE_VALUE###') F,
(SELECT COUNT(*) ED_EXIST FROM CDR.SPARK_IT_ZTE_EMERGENCY_DATA WHERE TRANSACTION_DATE='###SLICE_VALUE###') G,
(SELECT COUNT(*) LOAN_CDR_EXIST FROM CDR.SPARK_IT_ZTE_LOAN_CDR WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') M,
(SELECT COUNT(*) EC_EXIST FROM CDR.SPARK_IT_ZTE_EMERGENCY_CREDIT WHERE TRANSACTION_DATE='###SLICE_VALUE###') H,
(SELECT COUNT(*) BALRESET_EXIST FROM CDR.SPARK_IT_ZTE_BALANCE_RESET WHERE BAL_RESET_DATE='###SLICE_VALUE###') I,
(SELECT COUNT(*) DELEXPBAL_EXIST FROM CDR.SPARK_IT_ZTE_DEL_EXPBAL WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') J,
(SELECT COUNT(*) RECURRING_EXIST FROM CDR.SPARK_IT_ZTE_RECURRING WHERE EVENT_DATE='###SLICE_VALUE###') K,
(SELECT COUNT(*) PRICE_PLAN_EXIST FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') L,
(SELECT COUNT(A.FILE_NAME) MISSING_BALRESET FROM
(SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_BALRESET_CDR'
UNION
SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_BALRESET_CDR'
) A LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_BALANCE_RESET B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = A.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T6