
---------- Revenue Orange Money

SELECT SUM(REVENU), JOUR
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
WHERE JOUR >='2020-04-29' --and STYLE  IN ('RECHARGE','TOP_UP')
GROUP BY JOUR

----------- Stock total client OM
SELECT USER_DOMAIN,  SUM(BALANCE)
FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE ='2020-04-29' AND USER_CATEGORY IN ('Subscriber')
GROUP BY USER_DOMAIN



------------- -	Recharges (OM)

SELECT SUM(VAL), JOUR
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
WHERE JOUR ='2020-04-29' and STYLE  IN ('RECHARGE','TOP_UP')
GROUP BY JOUR

-------- Users
SELECT COUNT (DISTINCT MSISDN)
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
WHERE JOUR BETWEEN date_sub('2020-04-29',7) and '2020-04-29' AND STYLE NOT LIKE ('REC%')


----	Nombre de Pos OM actif  MONDV_OM.DATAMART_OM_DISTRIB

SELECT
    COUNT(DISTINCT DOD.MSISDN) PDV_30JRS
FROM
    MON.SPARK_DATAMART_OM_DISTRIB DOD
LEFT JOIN MON.SPARK_REF_OM_PRODUCTS RE
            ON DOD.MSISDN=RE.MSISDN
WHERE JOUR BETWEEN date_sub('2020-04-29',30) and '2020-04-29' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS where ref_date<='2020-04-29' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'