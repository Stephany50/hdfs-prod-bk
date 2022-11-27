SELECT IF(
T_1.TRANS_EXIST > 0 AND
T_3.BAL_EXIST > 0 AND
T_4.ACCT_EXIST > 0 AND
T_2.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) TRANS_EXIST FROM cdr.spark_it_omny_transactions WHERE original_file_date ='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) BAL_EXIST FROM cdr.spark_it_om_all_balance WHERE original_file_date ='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) ACCT_EXIST FROM cdr.spark_it_omny_account_snapshot_new WHERE original_file_date ='###SLICE_VALUE###') T_4,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2
