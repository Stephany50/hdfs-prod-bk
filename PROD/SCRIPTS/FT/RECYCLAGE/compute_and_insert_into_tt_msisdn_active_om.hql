INSERT INTO TMP.TT_MSISDN_ACTIVE_OM
SELECT A.*, current_timestamp INSERT_DATE FROM (
SELECT TO_DATE('###SLICE_VALUE###') EVENT_DATE, MSISDN, sum(transaction_amount) TRANSACTION_AMOUNT, max(last_transaction_date) LAST_TRANSACTION_DATE, sum(nb_count) NB_COUNT
FROM (
SELECT MSISDN,  TRANSACTION_AMOUNT, LAST_TRANSACTION_DATE, NB_COUNT
FROM TMP.TT_MSISDN_ACTIVE_OM WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1)
UNION ALL
SELECT SENDER_MSISDN, transaction_amount, FROM_UNIXTIME(unix_timestamp(transfer_datetime, 'yyyy-MM-dd HH:mm:ss')) last_transaction_date, 1 NB_COUNT
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME = TO_DATE('###SLICE_VALUE###')
AND TRANSFER_STATUS='TS'
UNION ALL
SELECT RECEIVER_MSISDN, transaction_amount, FROM_UNIXTIME(unix_timestamp(transfer_datetime, 'yyyy-MM-dd HH:mm:ss')) last_transaction_date, 1 NB_COUNT
FROM CDR.spark_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME = TO_DATE('###SLICE_VALUE###')
AND TRANSFER_STATUS='TS'
) a GROUP BY MSISDN
) A LEFT JOIN ( SELECT distinct EVENT_DATE FROM TMP.TT_MSISDN_ACTIVE_OM WHERE EVENT_DATE='###SLICE_VALUE###' ) b ON A.EVENT_DATE = B.EVENT_DATE
WHERE B.EVENT_DATE IS NULL