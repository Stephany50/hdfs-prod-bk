SELECT IF(T1.count_total = 0 or T2.count_total_ok > 0 , 'OK', 'NOK')
FROM
(select count(*) count_total from MON.SPARK_FT_DAILY_STATUS where  table_date = '###SLICE_VALUE###') T1,
(select count(*) count_total_ok from MON.SPARK_FT_DAILY_STATUS where  table_date = '###SLICE_VALUE###' AND UPPER(statut)<>'OK') T2