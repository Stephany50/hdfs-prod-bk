INSERT INTO SPOOL.SPOOL_OMNY_CHANNEL_USER_TRANS



SELECT
 
 TO_DATE(TRANSFER_DATETIME) TRANSFER_DATE,
 from_unixtime(cast(unix_timestamp(a.TRANSFER_DATETIME,'HH:mm:ss') as bigint),'HH:mm:ss')   TRANSFER_TIME,
 a.SENDER_MSISDN SENDER_MSISDN,
 b.LAST_NAME LAST_NAME,
 b.USER_NAME USER_NAME,
 b.USER_DOMAIN USER_DOMAIN,
 a.SENDER_CATEGORY_CODE SENDER_CATEGORY_CODE,
 a.RECEIVER_MSISDN RECEIVER_MSISDN_ACC,
 a.RECEIVER_CATEGORY_CODE RECIEVER_CATEGORY_CODE,
 a.TRANSACTION_AMOUNT TRANSACTION_AMOUNT,
 a.SERVICE_TYPE SERVICE_TYPE,
 a.TRANSFER_STATUS TRANSFER_STATUS,
 a.SENDER_PRE_BAL SENDER_PRE_BAL,
 a.SENDER_POST_BAL SENDER_POST_BAL,
 a.RECEIVER_PRE_BAL RECEIVER_PRE_BAL,
 a.RECEIVER_POST_BAL RECEIVER_POST_BAL,
 a.TRANSFER_ID TRANSFER_ID,
 CURRENT_TIMESTAMP() INSERT_DATE,
 '2020-02-02' EVENT_DATE
 
FROM
(SELECT * FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME = '2020-02-02' AND SERVICE_TYPE IN ('CASHIN','OPTW') ) a

INNER JOIN

(SELECT *

FROM CDR.SPARK_IT_OM_ALL_BALANCE

WHERE ORIGINAL_FILE_DATE=(SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE BETWEEN  DATE_SUB('2020-02-02', 7) AND '2020-02-02')

AND USER_DOMAIN IN ('TOTAL MARCHAND' ,'Total Partner','Key Account Partner1','Key Account Partner2')

) b

ON  a.SENDER_MSISDN = b.ACCOUNT_ID



UNION


SELECT
 
 TO_DATE(TRANSFER_DATETIME) TRANSFER_DATE,
 from_unixtime(cast(unix_timestamp(a.TRANSFER_DATETIME,'HH:mm:ss') as bigint),'HH:mm:ss')   TRANSFER_TIME, 
 a.SENDER_MSISDN,
 b.LAST_NAME,
 b.USER_NAME,
 b.USER_DOMAIN,
 a.SENDER_CATEGORY_CODE,
 a.RECEIVER_MSISDN RECEIVER_MSISDN_ACC,
 a.RECEIVER_CATEGORY_CODE RECIEVER_CATEGORY_CODE,
 a.TRANSACTION_AMOUNT,
 a.SERVICE_TYPE,
 a.TRANSFER_STATUS,
 a.SENDER_PRE_BAL,
 a.SENDER_POST_BAL,
 a.RECEIVER_PRE_BAL,
 a.RECEIVER_POST_BAL,
 a.TRANSFER_ID,
 CURRENT_TIMESTAMP() INSERT_DATE,
 '2020-02-02' EVENT_DATE
 
FROM
(SELECT * FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME = '2020-02-02'  AND SERVICE_TYPE IN ('O2C','CASHOUT','COUTBYCODE') ) a

INNER JOIN

(SELECT *

    FROM CDR.SPARK_IT_OM_ALL_BALANCE

    WHERE ORIGINAL_FILE_DATE=(SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB('2020-02-02', 7) AND '2020-02-02')

    AND USER_DOMAIN IN ('TOTAL MARCHAND' ,'Total Partner' ,'Key Account Partner1' ,'Key Account Partner2')

 ) b

ON  a.RECEIVER_MSISDN = b.ACCOUNT_ID
