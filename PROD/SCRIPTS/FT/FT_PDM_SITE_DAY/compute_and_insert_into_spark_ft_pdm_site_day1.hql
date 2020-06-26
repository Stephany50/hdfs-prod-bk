INSERT INTO MON.SPARK_FT_PDM_SITE_DAY
SELECT
count(distinct MSISDN) subscriber_count,
a.SITE_NAME SITE_NAME,
CURRENT_TIMESTAMP REFRESH_DATE,
network,
OTHER_NETWORK,
TOWNNAME,
ADMINISTRATIVE_REGION,
COMMERCIAL_REGION,
'###SLICE_VALUE###' as event_date
FROM
(
SELECT DISTINCT
MSISDN,
SITE_NAME,
'OCM' AS NETWORK,
CURRENT_TIMESTAMP REFRESH_DATE,
NETWORK AS OTHER_NETWORK
FROM
(
SELECT MSISDN, SITE_NAME, NETWORK
FROM
(
SELECT a.MSISDN,
b.SITE_NAME,
a.NETWORK,
ROW_NUMBER() OVER (PARTITION BY a.MSISDN, a.NETWORK ORDER BY SUM (NVL (DUREE_SORTANT, 0) + NVL (DUREE_ENTRANT, 0) + NVL (NBRE_SMS_SORTANT, 0)  + NVL (NBRE_SMS_ENTRANT, 0) ) DESC) AS RANG
FROM MON.SPARK_FT_PDM_OCM_SITE_DAY a
LEFT JOIN (SELECT CI, SITE_NAME FROM VW_SDT_CI_INFO_NEW) b
ON a.CI = b.CI
WHERE EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',89)  AND '###SLICE_VALUE###'
AND FN_NNP_SIMPLE_DESTINATION(MSISDN) = 'OCM'
GROUP BY a.MSISDN, b.SITE_NAME, a.NETWORK
) TT
WHERE RANG = 1
) T
) a
LEFT JOIN
(
SELECT SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
FROM VW_SITE_INFO
) b
ON a.SITE_NAME = b.SITE_NAME
GROUP BY  a.SITE_NAME,
network,
OTHER_NETWORK,
TOWNNAME,
ADMINISTRATIVE_REGION,
COMMERCIAL_REGION