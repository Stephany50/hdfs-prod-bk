SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT>0  and C.FT_COUNT>0 and D.FT_COUNT>0 , "OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_TT_ANALYSE_DECON_GLOBAL_CONSO1 WHERE EVENT_MONTH='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_CONSO_MSISDN_MONTH WHERE EVENT_MONTH='###SLICE_VALUE###') B,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION WHERE EVENT_MONTH='###SLICE_VALUE###') C,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY WHERE SMONTH='###SLICE_VALUE###') D

MON.SPARK_TT_GROUP_SUBS_ACCT_DISCONNECT
MON.TF_GLOBAL_CONSO_MSISDN_MONTH




