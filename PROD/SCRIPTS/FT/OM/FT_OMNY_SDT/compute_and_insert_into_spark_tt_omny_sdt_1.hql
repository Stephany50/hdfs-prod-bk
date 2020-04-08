INSERT INTO TMP.TT_OMNY_SDT_1
SELECT
    INS.*
    , PURD.*
    , (CASE WHEN PURD.RECEIVER_MSISDN IS NOT NULL THEN 'SD' ELSE 'S' END) REG_LEVEL
FROM
(
    SELECT 
        A.EVENT_DATE
        , A.MSISDN
        , A.USER_NAME
        , A.ADDRESS
        , A.DATE_INSCRIPT
        , A.INSCRIPTEUR
        , A.ID_NUMBER
        , A.SEX
        , A.DATE_OF_BIRTH
    FROM
    (
        SELECT
            TO_DATE(A.REGISTERED_ON) EVENT_DATE
            , A.MSISDN
            , CONCAT(CONCAT(A.USER_FIRST_NAME,' '), A.USER_LAST_NAME) USER_NAME
            , CONCAT(CONCAT(CONCAT(CONCAT(A.ADDRESS1,','), A.ADDRESS2),','), A.CITY) ADDRESS
            , TO_DATE(A.REGISTERED_ON) DATE_INSCRIPT
            , A.CREATED_BY_MSISDN INSCRIPTEUR
            , A.ID_NUMBER
            , A.SEX
            , A.DOB DATE_OF_BIRTH
            , ROW_NUMBER() OVER (PARTITION BY A.MSISDN ORDER BY A.REGISTERED_ON DESC) AS ROWNUMBER
        FROM CDR.SPARK_IT_OM_SUBSCRIBERS A
        WHERE TO_DATE(A.REGISTERED_ON) = "###SLICE_VALUE###" AND A.USER_TYPE='SUBSCRIBER'
    ) A
    WHERE ROWNUMBER = 1
) INS
LEFT JOIN 
(
    SELECT
        T.RECEIVER_MSISDN
        , MIN(T.TRANSFER_DATETIME_NQ) MIDATEDEP
        , SUM(T.TRANSACTION_AMOUNT) MTT_DEPOT
        , COUNT(T.TRANSFER_ID) NB_DEPOT
        , MIN(T.TRANSACTION_AMOUNT) FIRSTDEPOT
    FROM
    (
        SELECT
            T.RECEIVER_MSISDN
            , T.TRANSFER_DATETIME_NQ
            , T.TRANSACTION_AMOUNT
            , T.TRANSFER_ID
            , MIN(T.TRANSFER_DATETIME_NQ) OVER(PARTITION BY T.RECEIVER_MSISDN) AS MIN_TRANSFER_DATETIME_NQ
        FROM CDR.SPARK_IT_OMNY_TRANSACTIONS T
        WHERE T.TRANSFER_DATETIME = "###SLICE_VALUE###" AND T.TRANSFER_STATUS='TS' AND T.SERVICE_TYPE IN ('CASHIN','P2P','OTF','ENT2REG')
    ) T
    WHERE T.TRANSFER_DATETIME_NQ = T.MIN_TRANSFER_DATETIME_NQ
    GROUP BY RECEIVER_MSISDN
) PURD
ON INS.MSISDN = PURD.RECEIVER_MSISDN