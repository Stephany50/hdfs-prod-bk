SELECT IF(
T_1.FT_EXIST > 0 AND
T_2.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.spark_ft_billed_transaction_prepaid WHERE transaction_date ='###SLICE_VALUE###') T_1,
(SELECT count(*) NB_EXPORT from MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_EXPORT_TEST') T_2