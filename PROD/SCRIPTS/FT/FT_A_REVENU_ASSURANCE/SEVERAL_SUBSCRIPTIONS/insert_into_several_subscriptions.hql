INSERT INTO MON.SEVERAL_SUBSCRIPTIONS
SELECT
s.TRANSACTION_DATE TRANSACTION_DATE,
s.SUBSCRIPTION_SERVICE_DETAILS SUBSCRIPTION_SERVICE_DETAILS,
s.BENEFIT_BALANCE_LIST BENEFIT_BALANCE_LIST,
s.BENEFIT_UNIT_LIST BENEFIT_UNIT_LIST,
COUNT(s.SUBSCRIPTION_SERVICE_DETAILS) AS OCCURENCE,
ROUND( B.BORNE_SUP, 2) AS BORNE_SUP,
ROUND((COUNT(s.SUBSCRIPTION_SERVICE_DETAILS)-B.BORNE_SUP)/B.BORNE_SUP, 2) AS DISPERSION,
CURRENT_TIMESTAMP AS INSERT_DATE,
TO_DATE(TRANSACTION_DATE) EVENT_DATE
FROM mon.SPARK_FT_SUBSCRIPTION s
INNER JOIN
(SELECT A.SUBSCRIPTION_SERVICE_DETAILS, ROUND(STDDEV(A.OCCURENCE), 2) AS STANDARD_DEVIATION,

AVG(A.OCCURENCE) AS AVERAGE, ROUND((STDDEV(A.OCCURENCE) + AVG(A.OCCURENCE)), 2) AS BORNE_SUP

FROM

(SELECT s.TRANSACTION_DATE TRANSACTION_DATE, s.SUBSCRIPTION_SERVICE_DETAILS SUBSCRIPTION_SERVICE_DETAILS,

COUNT(s.SUBSCRIPTION_SERVICE_DETAILS) AS OCCURENCE

FROM mon.SPARK_FT_SUBSCRIPTION s

WHERE s.TRANSACTION_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',30) AND  DATE_SUB('###SLICE_VALUE###',1)

GROUP BY s.TRANSACTION_DATE, s.SUBSCRIPTION_SERVICE_DETAILS

HAVING COUNT(s.SUBSCRIPTION_SERVICE_DETAILS) > 20) A

GROUP BY A.SUBSCRIPTION_SERVICE_DETAILS) B

ON s.SUBSCRIPTION_SERVICE_DETAILS = B.SUBSCRIPTION_SERVICE_DETAILS

WHERE s.TRANSACTION_DATE= '###SLICE_VALUE###'

GROUP BY s.TRANSACTION_DATE, s.SUBSCRIPTION_SERVICE_DETAILS, s.BENEFIT_BALANCE_LIST, s.BENEFIT_UNIT_LIST, B.BORNE_SUP

HAVING COUNT(s.SUBSCRIPTION_SERVICE_DETAILS) >= B.BORNE_SUP

ORDER BY ROUND((COUNT(s.SUBSCRIPTION_SERVICE_DETAILS)-B.BORNE_SUP)/B.BORNE_SUP, 2) DESC