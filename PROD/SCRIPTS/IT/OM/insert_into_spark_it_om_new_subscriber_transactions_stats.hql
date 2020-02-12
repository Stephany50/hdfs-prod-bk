INSERT INTO  CDR.SPARK_IT_OM_NEW_SUBSCRIBER_TRANSACTIONS_STATS
SELECT
MSISDN,
CREATION_DATE,
COUNT (TRANSFER_ID) AS NB_OPERATIONS,
COUNT (DISTINCT SERVICE_TYPE) AS NB_SERVICES_DISTINCTS
FROM
      ((SELECT A1.CREATION_DATE AS CREATION_DATE , A1.MSISDN AS MSISDN, B1.TRANSFER_ID AS TRANSFER_ID, B1.SERVICE_TYPE AS SERVICE_TYPE
      FROM
           ( SELECT *
            FROM CDR.SPARK_IT_OM_SUBSCRIBERS
            WHERE CREATION_DATE >= 1577833200
            ) A1
      INNER JOIN CDR.SPARK_IT_OMNY_TRANSACTIONS B1 ON A1.MSISDN = B1.SENDER_MSISDN)
      UNION
      (SELECT A2.CREATION_DATE AS CREATION_DATE, A2.MSISDN AS MSISDN, B2.TRANSFER_ID AS TRANSFER_ID, B2.SERVICE_TYPE AS SERVICE_TYPE
      FROM
            ( SELECT *
            FROM CDR.SPARK_IT_OM_SUBSCRIBERS
            WHERE CREATION_DATE >= 1577833200
            ) A2
      INNER JOIN CDR.SPARK_IT_OMNY_TRANSACTIONS B2 ON A2.MSISDN = B2.RECEIVER_MSISDN)) C
GROUP BY C.MSISDN, CREATION_DATE