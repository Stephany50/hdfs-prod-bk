INSERT INTO AGG.SPARK_FT_A_DATA_TRANSFER
SELECT
    COUNT(*) NB_TRANSFERS,
    SUM(A.BYTES_DEBITED) BYTES_DEBITED,
    SUM(A.BYTES_CREDITED) BYTES_CREDITED,
    SUM(A.AMOUNT_CHARGED) MONTANT_FRAIS,
    (CASE 
        WHEN A.AMOUNT_CHARGED = 10 THEN '1 - 75 Mo'
        WHEN A.AMOUNT_CHARGED = 25 THEN '76 - 200 Mo'
        WHEN A.AMOUNT_CHARGED = 50 THEN '201 - 500 Mo'
        ELSE 'AUTRES'
    END) BUNDLE,
    MAX(TMP1.COUNT_SENDER_MSISDN) NB_DISTINCT_DAILY_SENDERS,
    MAX(TMP2.COUNT_RECEIVER_MSISDN) NB_DISTINCT_DAILY_RECEIVERS,
    MAX(TMP3.COUNT_DISTINCT_DAILY_USERS) NB_DISTINCT_DAILY_USERS,
    A.SENDER_OPERATOR_CODE,
    A.RECEIVER_OPERATOR_CODE,
    A.SENDER_OFFER_PROFILE_CODE,
    A.RECEIVER_OFFER_PROFILE_CODE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    A.TRANSACTION_DATE EVENT_DATE
FROM 
(
    SELECT *
    FROM MON.FT_DATA_TRANSFER
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
) A
LEFT JOIN (SELECT COUNT(DISTINCT SENDER_MSISDN) COUNT_SENDER_MSISDN FROM MON.FT_DATA_TRANSFER WHERE TRANSACTION_DATE = '###SLICE_VALUE###') TMP1
LEFT JOIN (SELECT COUNT(DISTINCT RECEIVER_MSISDN) COUNT_RECEIVER_MSISDN FROM MON.FT_DATA_TRANSFER WHERE TRANSACTION_DATE = '###SLICE_VALUE###') TMP2
LEFT JOIN (
SELECT COUNT(DISTINCT MSISDN) COUNT_DISTINCT_DAILY_USERS FROM
    (
    SELECT RECEIVER_MSISDN MSISDN FROM MON.FT_DATA_TRANSFER WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    UNION ALL
    SELECT SENDER_MSISDN MSISDN FROM MON.FT_DATA_TRANSFER WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    ) T
) TMP3
GROUP BY 
    A.TRANSACTION_DATE,     
    (CASE 
        WHEN A.AMOUNT_CHARGED = 10 THEN '1 - 75 Mo'
        WHEN A.AMOUNT_CHARGED = 25 THEN '76 - 200 Mo'
        WHEN A.AMOUNT_CHARGED = 50 THEN '201 - 500 Mo'
        ELSE 'AUTRES'
    END), 
    A.SENDER_OPERATOR_CODE,
    A.RECEIVER_OPERATOR_CODE,
    A.SENDER_OFFER_PROFILE_CODE,
    A.RECEIVER_OFFER_PROFILE_CODE

