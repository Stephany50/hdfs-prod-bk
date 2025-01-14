INSERT INTO SPOOL.SPOOL_CX
SELECT
A.TRANSFER_DATETIME_NQ AS TRANSFER_DATETIME,
A.SENDER_MSISDN,
C.SEGMENTATION,
A.RECEIVER_MSISDN,
A.TRANSACTION_TAG,
A.TRANSFER_STATUS,
A.ERROR_DESC,
CURRENT_TIMESTAMP() INSERT_DATE,
A.TRANSFER_DATETIME AS EVENT_DATE
FROM
    (SELECT * FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME = "###SLICE_VALUE###") A
    LEFT JOIN
    (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VVALUE###') B
    ON A.SENDER_MSISDN = B.ACCESS_KEY
    LEFT JOIN
    (SELECT * FROM DIM.SPARK_DT_OFFER_PROFILES) C
    ON B.COMMERCIAL_OFFER = C.PROFILE_CODE
