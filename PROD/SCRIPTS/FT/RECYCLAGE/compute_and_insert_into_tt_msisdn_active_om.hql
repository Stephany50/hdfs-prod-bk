insert into TMP.TT_MSISDN_ACTIVE_OM
SELECT A.*, current_timestamp INSERT_DATE FROM (
SELECT TO_DATE('###SLICE_VALUE###') EVENT_DATE, MSISDN, sum(transaction_amount) transaction_amount, max(last_transaction_date) last_transaction_date, sum(nb_count) nb_count
FROM (
SELECT MSISDN,  transaction_amount, last_transaction_date, nb_count
FROM TMP.TT_MSISDN_ACTIVE_OM WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1)
UNION ALL
SELECT SENDER_MSISDN, sum(transaction_amount) transaction_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_datetime, 'yyyy-MM-dd HH:mm:ss'))) last_transaction_date, count(*) nb_count
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME BETWEEN TO_DATE('###SLICE_VALUE###') AND TO_DATE('###SLICE_VALUE###')
AND TRANSFER_STATUS='TS'
group by SENDER_MSISDN
UNION ALL
SELECT RECEIVER_MSISDN, sum(transaction_amount) transaction_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_datetime, 'yyyy-MM-dd HH:mm:ss'))) last_transaction_date, count(*) nb_count
FROM CDR.spark_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME BETWEEN TO_DATE('###SLICE_VALUE###') and TO_DATE('###SLICE_VALUE###')
AND TRANSFER_STATUS='TS'
GROUP BY RECEIVER_MSISDN
) a GROUP BY MSISDN
) A LEFT JOIN ( SELECT EVENT_DATE FROM TMP.TT_MSISDN_ACTIVE_OM WHERE EVENT_DATE='###SLICE_VALUE###' ) b ON A.EVENT_DATE = B.EVENT_DATE
WHERE B.EVENT_DATE IS NULL