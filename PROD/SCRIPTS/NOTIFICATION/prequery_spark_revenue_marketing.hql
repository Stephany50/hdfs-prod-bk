SELECT  IF(T_1.REVENUE_EXISTS = 0 AND T_2.SOURCE_DATA=5 AND T_4.REVENUE_PRV_EXISTS >=1 ,"OK","NOK") REVENUE_EXISTS
FROM
(SELECT COUNT(*) REVENUE_EXISTS FROM MON.SPARK_REVENUE_MARKETING WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) REVENUE_PRV_EXISTS FROM MON.SPARK_REVENUE_MARKETING WHERE TRANSACTION_DATE=date_sub('###SLICE_VALUE###',1)) T_4,
(SELECT COUNT(DISTINCT SOURCE_DATA) SOURCE_DATA  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE' AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT','FT_OVERDRAFT','IT_ZTE_ADJUSTMENT')) T_2