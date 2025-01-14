SELECT
MSISDN,
SITE,
SITE_COD,
ADMINISTRATIVE_REGION,
REGION_PMO,
ZONE_PMO,
LONGITUDE,
LATITUDE,
SERVICE_TYPE,
DATE_TXT,
SUM(MONTANT_TRANSACTION)MONTANT_TRANSACTION,
COUNT(TRANSACTION_ID)TRANSACTION_ID,
SUM(REVENUS)REVENUS,
SUM(COMMISSION)COMMISSION
FROM
(
SELECT A.MSISDN, SERVICE_TYPE, DATE_TXT, TRANSACTION_ID, MONTANT_TRANSACTION, REVENUS, COMMISSION,SITE,SITE_COD,VILLE,ADMINISTRATIVE_REGION, REGION_PMO, ZONE_PMO, LONGITUDE, LATITUDE
FROM
(
SELECT TO_DATE(TRANSFER_DATETIME) DATE_TXT , SERVICE_TYPE, SENDER_MSISDN MSISDN, TRANSACTION_AMOUNT MONTANT_TRANSACTION, TRANSFER_ID TRANSACTION_ID,SERVICE_CHARGE_RECEIVED REVENUS, COMMISSIONS_PAID COMMISSION
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME = '###SLICE_VALUE###' 
AND SERVICE_TYPE IN ('CASHIN')
AND TRANSFER_STATUS = 'TS' AND SENDER_CATEGORY_CODE NOT IN ('SUBS')
UNION
SELECT TO_DATE(TRANSFER_DATETIME) DATE_TXT , SERVICE_TYPE, RECEIVER_MSISDN MSISDN, TRANSACTION_AMOUNT MONTANT_TRANSACTION, TRANSFER_ID TRANSACTION_ID, SERVICE_CHARGE_RECEIVED REVENUS, COMMISSIONS_PAID COMMISSION
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME = '###SLICE_VALUE###' 
AND SERVICE_TYPE IN ('CASHOUT','COUTBYCODE','O2C')
AND TRANSFER_STATUS = 'TS'
UNION
SELECT TO_DATE(TRANSFER_DATETIME) DATE_TXT , SERVICE_TYPE, SENDER_MSISDN MSISDN, TRANSACTION_AMOUNT MONTANT_TRANSACTION, TRANSFER_ID TRANSACTION_ID,
SERVICE_CHARGE_RECEIVED REVENUS, COMMISSIONS_PAID COMMISSION
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME = '###SLICE_VALUE###'
AND SERVICE_TYPE IN ('P2P')
AND TRANSFER_STATUS = 'TS' AND SENDER_CATEGORY_CODE NOT IN ('SUBS') AND RECEIVER_CATEGORY_CODE NOT IN ('SUBS')
) A
LEFT JOIN
(
SELECT MSISDN, SITE_NAME SITE, TOWNNAME VILLE, ADMINISTRATIVE_REGION, B.REGION_PMO, B.ZONE_LIB ZONE_PMO, B.LONGI LONGITUDE, B.LAT LATITUDE,B.SITE_COD
FROM
(
SELECT *
FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
WHERE EVENT_DATE='###SLICE_VALUE###'
) A
LEFT JOIN
(
SELECT SITE, SITE_COD, VILLE, REGION_PMO, ZONE_LIB, MAX(LONG) LONGI, MAX(LAT) LAT
FROM DIM.DT_SITE_ZONE 
GROUP BY SITE, VILLE, REGION_PMO,ZONE_LIB,SITE_COD
) B ON A.SITE_NAME = B.SITE
) B
ON A.MSISDN=B.MSISDN
)J
GROUP BY SERVICE_TYPE,MSISDN,DATE_TXT,SITE,VILLE,ADMINISTRATIVE_REGION,REGION_PMO,ZONE_PMO,LONGITUDE,LATITUDE,SITE_COD