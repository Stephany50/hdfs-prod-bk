INSERT INTO MON.SMS_FICHIERS
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    transaction_date
FROM(
    SELECT *
    FROM  dim.dt_smsnotification_recipient
    WHERE  actif='YES' AND type='FICHIERS_'
) A
LEFT JOIN (
  SELECT
        transaction_date,
        CONCAT(
        'LE M ',DATE_FORMAT(B.TRANSACTION_DATE,'dd/MM')

	    , ' \n' ,'-IN : ',CASE WHEN IN_F <> 0  THEN 'NOK' ELSE 'OK' END
		, ' \n' ,'-MSC: ',CASE WHEN MSC <>0  THEN 'NOK' ELSE 'OK' END
		, ' \n' ,'-MVAS: ',CASE WHEN MVAS <>0  THEN 'NOK' ELSE 'OK' END
		, ' \n' ,'-OTARIE: ',CASE WHEN OTARIE <>0  THEN 'NOK' ELSE 'OK' END
		, ' \n' ,'-OM: ',CASE WHEN OM <>0  THEN 'NOK' ELSE 'OK' END
		, ' \n' ,'-EXTRA: ',CASE WHEN EXTRA <>0  THEN 'NOK' ELSE 'OK' END
		)  SMS

        FROM(
            select
                '###SLICE_VALUE###' transaction_date,
                SUM(CASE  WHEN TABLE_SOURCE = 'IN' THEN 1 ELSE 0 END) IN_F,

				SUM(CASE  WHEN TABLE_SOURCE = 'MSC' THEN 1 ELSE 0 END) MSC,
				SUM(CASE  WHEN TABLE_SOURCE = 'MVAS'  THEN 1 ELSE 0 END) MVAS,
				SUM(CASE  WHEN TABLE_SOURCE = 'OTARIE'  THEN 1 ELSE 0 END) OTARIE,
				SUM(CASE  WHEN TABLE_SOURCE = 'OM'  THEN 1 ELSE 0 END) OM,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-21,9))='_EXTRACT_' OR UPPER(SUBSTR(FILE_NAME,-31,8))='_ETRACT_' THEN 1 ELSE 0 END) EXTRA

				FROM  mon.missing_files WHERE original_file_date = date_sub('###SLICE_VALUE###',1)

		) B
	) C