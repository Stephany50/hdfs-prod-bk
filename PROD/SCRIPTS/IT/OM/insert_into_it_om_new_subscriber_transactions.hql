
INSERT INTO  CDR.SPARK_IT_OM_NEW_SUBSCRIBER_TRANSACTIONS

SELECT
CURRENT_DATE() AS DATE_NOW,
MSISDN,
USER_ID,
MONTH (CURRENT_DATE ()) AS MONTH_NOW,
CREATION_DATE,
MODIFIED_ON,
DATEDIFF(C.CREATION_DATE, C.MODIFIED_ON) AS NB_ACTIVITY,
TRANSFER_STATUS,
SERVICE_CHARGE_RECEIVED,
TRANSACTION_AMOUNT,
TRANSFER_ID,
SERVICE_TYPE



FROM
      (
      (SELECT A1.MSISDN AS MSISDN, A1.USER_ID AS USER_ID, A1.CREATION_DATE AS CREATION_DATE,
      A1.MODIFIED_ON AS MODIFIED_ON, B1.TRANSFER_ID AS TRANSFER_ID, B1.SERVICE_TYPE AS SERVICE_TYPE,
      B1.TRANSFER_STATUS AS TRANSFER_STATUS, B1.TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
      B1.SERVICE_CHARGE_RECEIVED AS SERVICE_CHARGE_RECEIVED, B1.TRANSFER_ID AS TRANSFER_ID,
      B1.TRANSFER_ID AS TRANSFER_ID, B1.SERVICE_TYPE AS SERVICE_TYPE
      FROM
           ( SELECT *
            FROM CDR.IT_OM_SUBSCRIBERS
            WHERE CREATION_DATE >= 1577833200
            ) A1

      INNER JOIN CDR.IT_OMNY_TRANSACTIONS B1 ON A1.MSISDN = B1.SENDER_MSISDN)
      UNION
      (SELECT A2.MSISDN AS MSISDN, A2.USER_ID AS USER_ID, A2.CREATION_DATE AS CREATION_DATE,
      A2.MODIFIED_ON AS MODIFIED_ON, B2.TRANSFER_ID AS TRANSFER_ID, B2.SERVICE_TYPE AS SERVICE_TYPE,
      B2.TRANSFER_STATUS AS TRANSFER_STATUS, B2.TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
      B2.SERVICE_CHARGE_RECEIVED AS SERVICE_CHARGE_RECEIVED,  B2.TRANSFER_ID AS TRANSFER_ID,
      B2.TRANSFER_ID AS TRANSFER_ID, B2.SERVICE_TYPE AS SERVICE_TYPE
      FROM
            ( SELECT *
            FROM CDR.IT_OM_SUBSCRIBERS
            WHERE CREATION_DATE >= 1577833200
            ) A2

      INNER JOIN CDR.IT_OMNY_TRANSACTIONS B2 ON A2.MSISDN = B2.RECEIVER_MSISDN)
      ) C
ORDER BY C.MSISDN





