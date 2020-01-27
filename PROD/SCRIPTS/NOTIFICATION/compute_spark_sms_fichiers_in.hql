INSERT INTO MON.SPARK_SMS_FICHIERS_IN
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    transaction_date
FROM(
    SELECT *
    FROM  dim.dt_smsnotification_recipient
    WHERE type='FICHIERS_IN' AND actif='YES'
) A
LEFT JOIN (
  SELECT
        transaction_date,
        CONCAT(
        'LE  ',DATE_FORMAT('###SLICE_VALUE###','dd/MM')
		, ' \n' ,NB_TOTAL,' Fichiers manquant'
        , ' \n' ,'*SOURCE :  ','IN,ZEBRA'


	    ,(case when DATA<>0 then '\n' || '-DATA : ' || DATA else '' end)

		,(case when SMS<>0 then '\n' || '-VOIX/SMS: ' || SMS else '' end)

		,(case when SUBSCRIB<>0  then '\n' || '-SUBSCRIB: ' || SUBSCRIB  else '' end)

		,(case when EMER_CREDIT<>0  then '\n' || '-EMER_CREDIT: ' || EMER_CREDIT  else '' end)

		,(case when EMER_DATA<>0  then '\n' || '-EMER_DATA: ' || EMER_DATA  else '' end)

		,(case when AJUSTMENT<>0  then '\n' || '-AJUSTMENT: ' || AJUSTMENT  else '' end)

		,(case when RECHARGE<>0  then '\n' || '-RECHARGE: ' || RECHARGE  else '' end)

		,(case when EXTRA<>0  then '\n' || '-EXTRACT: ' || EXTRA  else '' end)

		,(case when PROFILE<>0  then '\n' || '-PROFILE: ' || PROFILE  else '' end)

		,(case when DATA_POST<>0  then '\n' || '-DATA_POST: ' || DATA_POST  else '' end)
		,(case when ZEBRA<>0  then '\n' || '-ZEBRA: ' || ZEBRA  else '' end)

		,(case when TRANSFER<>0  then '\n' || '-TRANSFERT: ' || TRANSFER else '' end)
		)  SMS

        FROM(
            select
                '###SLICE_VALUE###' transaction_date,
                SUM(CASE  WHEN TABLE_SOURCE = 'IN' THEN 1 ELSE 0 END) NB_TOTAL,

				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-34,6))='_DATA_' THEN 1 ELSE 0 END) DATA,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-33,5))='_SMS_' THEN 1 ELSE 0 END) SMS,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-33,14))='_SUBSCRIPTION_' THEN 1 ELSE 0 END) SUBSCRIB,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,1,9))='IN_PR_EC_' THEN 1 ELSE 0 END) EMER_CREDIT,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,1,9))='IN_PR_ED_' THEN 1 ELSE 0 END)  EMER_DATA,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-33,5))='_ADJUSTMENT_' THEN 1 ELSE 0 END) AJUSTMENT,
				SUM(CASE  WHEN TABLE_SOURCE = 'ZEBRA'  THEN 1 ELSE 0 END) ZEBRA,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-21,9))='_RECHARGE_' THEN 1 ELSE 0 END) RECHARGE,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,-21,9))='_EXTRACT_' OR UPPER(SUBSTR(FILE_NAME,-31,8))='_ETRACT_' THEN 1 ELSE 0 END) EXTRA,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,1,8))='PROFILE_' THEN 1 ELSE 0 END) PROFILE,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,18,6))='_DATA_' THEN 1 ELSE 0 END) DATA_POST,
				SUM(CASE  WHEN TABLE_SOURCE = 'IN' AND UPPER(SUBSTR(FILE_NAME,1,12))='IN_PR_TRANSFER_' THEN 1 ELSE 0 END) TRANSFER

				FROM  mon.spark_missing_files WHERE original_file_date = '###SLICE_VALUE###'

		) B
	) C