 INSERT INTO MON.SPARK_SMS_SPOOL
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    transaction_date
FROM(
    SELECT *
    FROM  dim.dt_smsnotification_recipient
    WHERE  actif='YES' AND type='SPOOL'
) A
LEFT JOIN (
  SELECT
      -- transaction_date,
        CONCAT(
        'LE  ',DATE_FORMAT('###SLICE_VALUE###','dd/MM')
	    , ' \n' ,'-RIA : ',CASE WHEN (RIA1 + RIA2 + RIA3 + RIA4 + RIA5 + RIA6 ) >=2  THEN 'OK' ELSE 'NOK' END,'(',(RIA1+RIA2+RIA3+RIA4+RIA5+RIA6),'/6',')',' | ',CASE WHEN (RIA1_old +  RIA2_old + RIA3_old + RIA4_old + RIA5_old + RIA6_old ) >=2  THEN 'OK' ELSE 'NOK' END,'(',(RIA1_old+RIA2_old+RIA3_old+RIA4_old+RIA5_old+RIA6_old),'/6',')'
	    , ' \n' ,'-CBM : ',CASE WHEN (CBM1 + CBM2 + CBM3 + CBM4 )>=2  THEN 'OK' ELSE 'NOK' END,'(',(CBM1+CBM2+CBM3+CBM4),'/4',')',' | ',CASE WHEN (CBM1_old + CBM2_old + CBM3_old + CBM4_old)>=2  THEN 'OK' ELSE 'NOK' END,'(',(CBM1_old+CBM2_old+CBM3_old+CBM4_old),'/4',')'
	    , ' \n' ,'-SNAPSHOT : ',CASE WHEN SNAPSHOT <> 0  THEN 'OK' ELSE 'NOK' END,'(',(SNAPSHOT),'/1',')', ' | ' ,CASE WHEN SNAPSHOT_old <> 0  THEN 'OK' ELSE 'NOK' END,'(',(SNAPSHOT_old),'/1',')'
		
		)  SMS,
		'###SLICE_VALUE###' AS transaction_date
        FROM
            (select
                --'2019-10-27' log_date,
                SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,9)) = 'SPOOL_EVD' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA1,
				
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,20)) = 'SPOOL_ACTIVATIONSEVD' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA2,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,18)) = 'SPOOL_TRANSACTIONS' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA3,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,17)) = 'SPOOL_COMMISSIONS' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA4,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,19)) = 'SPOOL_ACTIVATIONSOM' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA5,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,19)) = 'SPOOL_ACTIVATIONSOM' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA6,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,14)) = 'SPOOL_CBM_DOLA' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM1,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,15)) = 'SPOOL_CBM_CHURN' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM2,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,22)) = 'SPOOL_CBM_CUST_INSIGTH' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM3,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,21)) = 'SPOOL_CBM_BUNDLE_SUBS' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM4,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,14)) = 'SPOOL_CONTRACT' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) SNAPSHOT
				

				FROM  cdr.spark_it_logs_spools WHERE
				(to_date(from_unixtime(unix_timestamp(substr(filename, -15, 8), 'yyyyMMdd'))) =DATE_SUB('###SLICE_VALUE###',1) AND UPPER(SUBSTR(filetype,1,14)) <> 'SPOOL_CONTRACT'  and UPPER(status)='SUCCESS') 
				OR 
				(DATE_ADD(to_date(from_unixtime(unix_timestamp(substr(filename, -15, 8), 'yyyyMMdd'))),1) = '###SLICE_VALUE###' AND UPPER(SUBSTR(filetype,1,14)) = 'SPOOL_CONTRACT'  and UPPER(status)='SUCCESS') 
				
				) news,
				
				
				
				 (select
                --'2019-10-28' log_date,
                SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,9)) = 'SPOOL_EVD' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA1_old,
				
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,20)) = 'SPOOL_ACTIVATIONSEVD' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA2_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,18)) = 'SPOOL_TRANSACTIONS' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA3_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,17)) = 'SPOOL_COMMISSIONS' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA4_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,19)) = 'SPOOL_ACTIVATIONSOM' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA5_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,19)) = 'SPOOL_ACTIVATIONSOM' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) RIA6_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,14)) = 'SPOOL_CBM_DOLA' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM1_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,15)) = 'SPOOL_CBM_CHURN' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM2_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,22)) = 'SPOOL_CBM_CUST_INSIGTH' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM3_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,21)) = 'SPOOL_CBM_BUNDLE_SUBS' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) CBM4_old,
				SUM(CASE  WHEN UPPER(SUBSTR(filetype,1,14)) = 'SPOOL_CONTRACT' AND upper(STATUS)='SUCCESS'  THEN 1 ELSE 0 END) SNAPSHOT_old
				

				FROM  cdr.spark_it_logs_spools WHERE
				(DATE_SUB(to_date(from_unixtime(unix_timestamp(substr(filename, -15, 8), 'yyyyMMdd'))),1) = DATE_SUB('###SLICE_VALUE###',2) AND UPPER(SUBSTR(filetype,1,14)) <> 'SPOOL_CONTRACT'  and UPPER(status)='SUCCESS') 
				OR 
				(DATE_SUB(to_date(from_unixtime(unix_timestamp(substr(filename, -15, 8), 'yyyyMMdd'))),1) = DATE_SUB('###SLICE_VALUE###',1) AND UPPER(SUBSTR(filetype,1,14)) = 'SPOOL_CONTRACT'  and UPPER(status)='SUCCESS')
				) H_old
	) B