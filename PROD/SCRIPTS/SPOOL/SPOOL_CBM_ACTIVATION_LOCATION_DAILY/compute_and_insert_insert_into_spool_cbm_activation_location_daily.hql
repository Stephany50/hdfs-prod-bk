add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar;
create temporary function FN_NNP_SIMPLE_DESTINATION as 'cm.orange.bigdata.udf.GetNnpSimpleDestn';
SELECT
EVENT_DATE 
MSISDN,
ACCOUNT_FORMULE,
OSP_CONTRACT_TYPE,
SITE_NAME AS FIRST_SITE_NAME,
TOWNNAME,
ADMINISTRATIVE_REGION,
CATEGORIES_SITE

FROM
(
SELECT  ACTIVATION_DATE AS EVENT_DATE,
MSISDN,
ACCOUNT_FORMULE,
OSP_CONTRACT_TYPE,
FIRST_SITE_NAME,
CATEGORIES_SITE
FROM MON.SPARK_FT_ACTIVATIONS_SITE_DAY
WHERE ACTIVATION_DATE = '2020-01-05'
AND DECODE (NVL(OSP_STATUS,CURRENT_STATUS), 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
, 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
, NVL(OSP_STATUS,CURRENT_STATUS ))='ACTIF'
AND MON.FN_NNP_SIMPLE_DESTINATION(MSISDN) = 'OCM'
) a
RIGHT JOIN
(
SELECT SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
FROM (	
SELECT site_name, townname, administrative_region, commercial_region
FROM (SELECT site_name, townname, region administrative_region,
commercial_region,
ROW_NUMBER () OVER (PARTITION BY site_name ORDER BY townname)
	  AS rang
FROM dim.dt_gsm_cell_code)
WHERE rang = 1
)
) b
WHERE a.FIRST_SITE_NAME = SITE_NAME