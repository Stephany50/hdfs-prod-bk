SELECT IF(A.nbr = 0  and B.nbr > 0 and C.nbr > 0 and D.nbr > 0 and E.nbr > 0,'OK','NOK') FROM
(SELECT COUNT(*) nbr FROM MON.SPARK_FT_SUIVI_TDD where stat_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_SELL_CUST_DAILY WHERE transaction_date='###SLICE_VALUE###') B,
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_DAILY_EQUIMENT_SOLD where stat_date='###SLICE_VALUE###') C,
(SELECT COUNT(*) nbr FROM Mon.Spark_Ft_client_last_site_day where event_date = '###SLICE_VALUE###') D,
(SELECT COUNT(*) nbr FROM Mon.Spark_FT_IMEI_ONLINE where sdate='###SLICE_VALUE###') E