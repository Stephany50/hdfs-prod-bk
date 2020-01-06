add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
CREATE TEMPORARY FUNCTION FN_GET_OPERATOR_CODE as 'cm.orange.bigdata.udf.GetOperatorCode';
CREATE TEMPORARY FUNCTION FN_GET_NNP_MSISDN_SIMPLE_DESTN as 'cm.orange.bigdata.udf.GetNnpMsisdnSimpleDestn';
CREATE TABLE tmp.msisdn_vue_360_7 AS
SELECT
    a.*,
    LOC_SITE_NAME,
    LOC_TOWN_NAME,
    LOC_ADMINTRATIVE_REGION,
    LOC_COMMERCIAL_REGION
FROM tmp.msisdn_vue_360_6 a
LEFT JOIN
(
    SELECT
        (CASE
            WHEN LENGTH(MSISDN) = 13 AND substr(MSISDN,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(MSISDN) IN ('MTN','VIETTEL','OCM') THEN SUBSTR(MSISDN,-9)
            ELSE MSISDN
        END) MSISDN,
        SITE_NAME AS LOC_SITE_NAME,
        TOWNNAME AS LOC_TOWN_NAME,
        ADMINISTRATIVE_REGION AS LOC_ADMINTRATIVE_REGION,
        COMMERCIAL_REGION AS LOC_COMMERCIAL_REGION
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
    WHERE EVENT_DATE = '2019-10-02'
    AND FN_GET_OPERATOR_CODE(MSISDN) IN ('SET', 'OCM')
)b on a.MSISDN=b.MSISDN