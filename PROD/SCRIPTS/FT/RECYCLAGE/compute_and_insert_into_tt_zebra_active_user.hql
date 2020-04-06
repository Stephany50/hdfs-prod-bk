<<<<<<< Updated upstream
INSERT INTO TMP.TT_ZEBRA_ACTIVE_USER
SELECT A.*, current_timestamp INSERT_DATE FROM (
SELECT '###SLICE_VALUE###' event_date, sender_msisdn, sum(nbre_transaction) nbre_transaction, sum(nbre_jour)nbre_jour, sum(total_transfer_amount)total_transfer_amount, max(last_transfer_date) last_transfer_date
FROM (
SELECT to_date(EVENT_DATE), sender_msisdn, NBRE_TRANSACTION, NBRE_JOUR, TOTAL_TRANSFER_AMOUNT, FROM_UNIXTIME(unix_timestamp(last_transfer_date, 'yyyy-MM-dd HH:mm:ss')) last_transfer_date
FROM TMP.TT_ZEBRA_ACTIVE_USER WHERE event_date='###SLICE_VALUE###'
UNION ALL
SELECT max(transfer_date) event_date, sender_msisdn, count(distinct transfer_id) nbre_transaction, count(distinct transfer_date) nbre_jour, sum(transfer_amount) total_transfer_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_date_time, 'yyyy-MM-dd HH:mm:ss'))) last_transfer_date
FROM CDR.SPARK_IT_ZEBRA_TRANSAC
WHERE transfer_date ='###SLICE_VALUE###'
group by sender_msisdn
) a  GROUP BY sender_msisdn
) A LEFT JOIN ( SELECT distinct EVENT_DATE FROM TMP.TT_ZEBRA_ACTIVE_USER WHERE EVENT_DATE='###SLICE_VALUE###' ) b ON A.EVENT_DATE = B.EVENT_DATE
WHERE B.EVENT_DATE IS NULL
=======
INSERT INTO TMP.TT_ZEBRA_ACTIVE_USER
SELECT A.*, current_timestamp INSERT_DATE FROM (
SELECT '###SLICE_VALUE###' event_date, sender_msisdn, sum(nbre_transaction) nbre_transaction, sum(nbre_jour)nbre_jour, sum(total_transfer_amount)total_transfer_amount, max(last_transfer_date) last_transfer_date
FROM (
SELECT to_date(EVENT_DATE), sender_msisdn, NBRE_TRANSACTION, NBRE_JOUR, TOTAL_TRANSFER_AMOUNT, FROM_UNIXTIME(unix_timestamp(last_transfer_date, 'yyyy-MM-dd HH:mm:ss')) last_transfer_date
FROM TMP.TT_ZEBRA_ACTIVE_USER WHERE event_date='###SLICE_VALUE###'
UNION ALL
SELECT max(transfer_date) event_date, sender_msisdn, count(distinct transfer_id) nbre_transaction, count(distinct transfer_date) nbre_jour, sum(transfer_amount) total_transfer_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_date_time, 'yyyy-MM-dd HH:mm:ss'))) last_transfer_date
FROM CDR.SPARK_IT_ZEBRA_TRANSAC
WHERE transfer_date ='###SLICE_VALUE###'
group by sender_msisdn
) a  GROUP BY sender_msisdn
) A LEFT JOIN ( SELECT distinct EVENT_DATE FROM TMP.TT_ZEBRA_ACTIVE_USER WHERE EVENT_DATE='###SLICE_VALUE###' ) b ON A.EVENT_DATE = B.EVENT_DATE
WHERE B.EVENT_DATE IS NULL
>>>>>>> Stashed changes
