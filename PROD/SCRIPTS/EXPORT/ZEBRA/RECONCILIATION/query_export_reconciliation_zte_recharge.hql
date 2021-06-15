SELECT 
DISTINCT SUBSTR(TRANSACTIONSN, 1, 19) TRANSACTIONSN
FROM CDR.SPARK_IT_ZTE_RECHARGE A
WHERE PAY_DATE="###SLICE_VALUE###" AND CHANNEL_ID=11
UNION ALL
SELECT
DISTINCT SUBSTR(TRANSACTIONSN, 1, 19) TRANSACTIONSN
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT A
WHERE CREATE_DATE="###SLICE_VALUE###" AND CHANNEL_ID=11 AND TRANSACTIONSN LIKE 'C%R'