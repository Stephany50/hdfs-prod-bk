SELECT IF(FT_CLIENT_LAST_SITE_LOCATION>0 AND FT_CONSO_MSISDN_DAY=30 AND FT_USERS_REGION_LOCATION=0, 'OK','NOK') STATUT FROM
(SELECT COUNT(*) FT_USERS_REGION_LOCATION FROM MON.SPARK_FT_USERS_REGION_LOCATION WHERE EVENT_DATE='###SLICE_VALUE###')A,
(SELECT COUNT(DISTINCT EVENT_DATE) FT_CONSO_MSISDN_DAY FROM MON.SPARK_FT_CONSO_MSISDN_DAY WHERE EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 29) AND '###SLICE_VALUE###')B,
(SELECT COUNT(*) FT_CLIENT_LAST_SITE_LOCATION FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION WHERE EVENT_MONTH = SUBSTRING(add_months('###SLICE_VALUE###',-1),0,7))C

