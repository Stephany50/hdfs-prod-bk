SELECT IF(T_1.SPOOL_COUNT = 0
 AND T_2.FT_SUBSCRIPTION > datediff(last_day(concat('###SLICE_VALUE###','-01'))
AND T_3.FT_ACCOUNT_ACTIVITY > 0
AND T_4.FT_CLIENT_LAST_SITE_LOCATION> 0
AND T_5.FT_IMEI_TRAFFIC_MONTHLY > 0
AND T_6.FT_CONSO_MSISDN_MONTH>0
AND T_7.FT_DATA_CONSO_MSISDN_MONTH>0
AND T_8.FT_SUBSCRIPTION_MSISDN_MONTH>0
, "OK", "NOK") FT_EXIST
FROM
(SELECT COUNT(*) SPOOL_COUNT FROM MON.SPARK_TF_BASE_GROUP_CONSO_MONTH WHERE BASE_MONTH = '###SLICE_VALUE###' LIMIT 10) T_1,
(SELECT COUNT(DISTINCT TRANSACTION_DATE) FT_SUBSCRIPTION FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE BETWEEN to_date('###SLICE_VALUE###'||'-01') AND LAST_DAY('###SLICE_VALUE###'||'-01') LIMIT 10) T_2,
(SELECT COUNT(*) FT_ACCOUNT_ACTIVITY FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE = DATE_SUB(LAST_DAY('###SLICE_VALUE###'),-1) LIMIT 10) T_3,
(SELECT COUNT(*) FT_CLIENT_LAST_SITE_LOCATION FROM mon.SPARK_FT_CLIENT_LAST_SITE_LOCATION WHERE EVENT_MONTH =substring('###SLICE_VALUE###',1,7) LIMIT 10) T_4,
(SELECT COUNT(*) FT_IMEI_TRAFFIC_MONTHLY FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY WHERE smonth =substring('###SLICE_VALUE###',1,7) LIMIT 10) T_5,
(SELECT COUNT(*) FT_CONSO_MSISDN_MONTH FROM MON.SPARK_FT_CONSO_MSISDN_MONTH WHERE EVENT_MONTH =substring('###SLICE_VALUE###',1,7) LIMIT 10) T_6,
(SELECT COUNT(*) FT_DATA_CONSO_MSISDN_MONTH FROM MON.SPARK_FT_DATA_CONSO_MSISDN_MONTH WHERE EVENT_MONTH =substring('###SLICE_VALUE###',1,7) LIMIT 10) T_7,
(SELECT COUNT(*) FT_SUBSCRIPTION_MSISDN_MONTH FROM MON.SPARK_FT_SUBSCRIPTION_MSISDN_MONTH WHERE EVENT_MONTH =substring('###SLICE_VALUE###',1,7) LIMIT 10) T_8
