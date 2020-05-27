INSERT INTO MON.SPARK_FT_OPERATOR_ACCT_ACTIVITY
SELECT
nvl(a.msisdn, b.msisdn) MSISDN,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.OG_CALL_1, b.OG_CALL_1),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.OG_CALL_1) AS string),CAST(TO_DATE(a.OG_CALL_2) AS string), CAST(TO_DATE(a.OG_CALL_3) AS string), CAST(TO_DATE(a.OG_CALL_4) AS string), CAST(TO_DATE(b.OG_CALL_1) AS string), CAST(TO_DATE(b.OG_CALL_2) AS string), CAST(TO_DATE(b.OG_CALL_3) AS string), CAST(TO_DATE(b.OG_CALL_4) AS string)), 4, 'DESC','\\|')))OG_CALL_1,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.OG_CALL_2, b.OG_CALL_2),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.OG_CALL_1) AS string),CAST(TO_DATE(a.OG_CALL_2) AS string), CAST(TO_DATE(a.OG_CALL_3) AS string), CAST(TO_DATE(a.OG_CALL_4) AS string), CAST(TO_DATE(b.OG_CALL_1) AS string), CAST(TO_DATE(b.OG_CALL_2) AS string), CAST(TO_DATE(b.OG_CALL_3) AS string), CAST(TO_DATE(b.OG_CALL_4) AS string)), 3, 'DESC','\\|')))OG_CALL_2,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.OG_CALL_3, b.OG_CALL_3),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.OG_CALL_1) AS string),CAST(TO_DATE(a.OG_CALL_2) AS string), CAST(TO_DATE(a.OG_CALL_3) AS string), CAST(TO_DATE(a.OG_CALL_4) AS string), CAST(TO_DATE(b.OG_CALL_1) AS string), CAST(TO_DATE(b.OG_CALL_2) AS string), CAST(TO_DATE(b.OG_CALL_3) AS string), CAST(TO_DATE(b.OG_CALL_4) AS string)), 2, 'DESC','\\|')))OG_CALL_3,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.OG_CALL_4, b.OG_CALL_4),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.OG_CALL_1) AS string),CAST(TO_DATE(a.OG_CALL_2) AS string), CAST(TO_DATE(a.OG_CALL_3) AS string), CAST(TO_DATE(a.OG_CALL_4) AS string), CAST(TO_DATE(b.OG_CALL_1) AS string), CAST(TO_DATE(b.OG_CALL_2) AS string), CAST(TO_DATE(b.OG_CALL_3) AS string), CAST(TO_DATE(b.OG_CALL_4) AS string)), 1, 'DESC','\\|')))OG_CALL_4,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.IC_CALL_1, b.IC_CALL_1),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.IC_CALL_1) AS string),CAST(TO_DATE(a.IC_CALL_2) AS string), CAST(TO_DATE(a.IC_CALL_3) AS string), CAST(TO_DATE(a.IC_CALL_4) AS string), CAST(TO_DATE(b.IC_CALL_1) AS string), CAST(TO_DATE(b.IC_CALL_2) AS string), CAST(TO_DATE(b.IC_CALL_3) AS string), CAST(TO_DATE(b.IC_CALL_4) AS string)), 4, 'DESC','\\|')))IC_CALL_1,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.IC_CALL_2, b.IC_CALL_2),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.IC_CALL_1) AS string),CAST(TO_DATE(a.IC_CALL_2) AS string), CAST(TO_DATE(a.IC_CALL_3) AS string), CAST(TO_DATE(a.IC_CALL_4) AS string), CAST(TO_DATE(b.IC_CALL_1) AS string), CAST(TO_DATE(b.IC_CALL_2) AS string), CAST(TO_DATE(b.IC_CALL_3) AS string), CAST(TO_DATE(b.IC_CALL_4) AS string)), 3, 'DESC','\\|')))IC_CALL_2,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.IC_CALL_3, b.IC_CALL_3),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.IC_CALL_1) AS string),CAST(TO_DATE(a.IC_CALL_2) AS string), CAST(TO_DATE(a.IC_CALL_3) AS string), CAST(TO_DATE(a.IC_CALL_4) AS string), CAST(TO_DATE(b.IC_CALL_1) AS string), CAST(TO_DATE(b.IC_CALL_2) AS string), CAST(TO_DATE(b.IC_CALL_3) AS string), CAST(TO_DATE(b.IC_CALL_4) AS string)), 2, 'DESC','\\|')))IC_CALL_3,
IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.IC_CALL_4, b.IC_CALL_4),TO_DATE ( fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(a.IC_CALL_1) AS string),CAST(TO_DATE(a.IC_CALL_2) AS string), CAST(TO_DATE(a.IC_CALL_3) AS string), CAST(TO_DATE(a.IC_CALL_4) AS string), CAST(TO_DATE(b.IC_CALL_1) AS string), CAST(TO_DATE(b.IC_CALL_2) AS string), CAST(TO_DATE(b.IC_CALL_3) AS string), CAST(TO_DATE(b.IC_CALL_4) AS string)), 1, 'DESC','\\|')))IC_CALL_4,
a.FIRST_ENTRY_DATE,
IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_1_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('1970-01-01')),NVL(b.IC_CALL_4,to_date('1970-01-01')))>=ADD_MONTHS(TO_DATE ('###SLICE_VALUE###'),-1) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_1_MONTH,
IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_2_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('1970-01-01')),NVL(b.IC_CALL_4,to_date('1970-01-01')))>=ADD_MONTHS(TO_DATE ('###SLICE_VALUE###'),-2) THEN 'ACTIVE' ELSE 'INACTIVE' END))  STATUS_2_MONTH,
IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_3_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('1970-01-01')),NVL(b.IC_CALL_4,to_date('1970-01-01')))>=ADD_MONTHS(TO_DATE ('###SLICE_VALUE###'),-3) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_3_MONTH,
IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_6_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('1970-01-01')),NVL(b.IC_CALL_4,to_date('1970-01-01')))>=ADD_MONTHS(TO_DATE ('###SLICE_VALUE###'),-6) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_6_MONTH,
IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_12_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('1970-01-01')),NVL(b.IC_CALL_4,to_date('1970-01-01')))>=ADD_MONTHS(TO_DATE ('###SLICE_VALUE###'),-12) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_12_MONTH,
a.SUB_OPERATOR_CODE,
a.OPERATOR_CODE,
CURRENT_TIMESTAMP INSERT_DATE,
'###SLICE_VALUE###' EVENT_DATE
FROM
(SELECT * FROM MON.SPARK_FT_OPERATOR_ACCT_ACTIVITY
WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) a
FULL OUTER JOIN
(
SELECT
  MSISDN
, max(OG_CALL_1)OG_CALL_1
, max(OG_CALL_2)OG_CALL_2
, max(OG_CALL_3)OG_CALL_3
, max(OG_CALL_4)OG_CALL_4
, max(IC_CALL_1)IC_CALL_1
, max(IC_CALL_2)IC_CALL_2
, max(IC_CALL_3)IC_CALL_3
, max(IC_CALL_4)IC_CALL_4
, max(OPERATOR_CODE)OPERATOR_CODE
, EVENT_DATE
FROM MON.SPARK_FT_HUA_OPERATOR_OG_IC_SNAPSHOT
WHERE EVENT_DATE='###SLICE_VALUE###'
GROUP BY EVENT_DATE,MSISDN
) b
ON ( a.msisdn = b.msisdn)