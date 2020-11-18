SELECT IF(
T_1.FT_COUNT > 0 AND
T_2.FT_COUNT > 0 AND
T_3.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_OM_BICEC_TRANSACTION WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_OM_BICEC_TRANS_RECONCILIATION' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_3