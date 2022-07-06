INSERT INTO TMP.MSISDNS_INTERACTION1
SELECT msisdn, other_msisdn, transaction_date, transaction_type 
FROM	
(
	SELECT 
	(
		CASE WHEN served_msisdn LIKE '1602%' 
		THEN SUBSTRING(served_msisdn, 5, 14) ELSE fn_format_msisdn_to_9digits(served_msisdn) END 
	) msisdn, 
	(
		CASE WHEN length(trim(fn_format_msisdn_to_9digits(other_party))) = 9 
		THEN fn_format_msisdn_to_9digits(other_party) ELSE SUBSTRING(other_party, 5, 14) END
	) other_msisdn, 
	transaction_type, transaction_date 
	FROM
	(
		SELECT * FROM MON.SPARK_FT_MSC_TRANSACTION
		WHERE 
		(length(trim(fn_format_msisdn_to_9digits(served_msisdn))) = 9 or served_msisdn like '1602%')
		AND (length(trim(fn_format_msisdn_to_9digits(other_party))) = 9 or other_party like '1601%' or other_party like '1603%')
		AND ( transaction_date BETWEEN date_sub(concat('###SLICE_VALUE###', '-01'), ${hivevar:nb_jour_30}) AND last_day(concat('###SLICE_VALUE###', '-01')) )
	)
)
UNION ALL
(
	SELECT 
	(
		CASE WHEN other_party LIKE '1602%' 
		THEN SUBSTRING(other_party, 5, 14) ELSE fn_format_msisdn_to_9digits(other_party) END 
	) msisdn,
	(
		CASE WHEN length(trim(fn_format_msisdn_to_9digits(served_msisdn))) = 9 
		THEN fn_format_msisdn_to_9digits(served_msisdn) ELSE SUBSTRING(served_msisdn, 5, 14) END
	) other_msisdn, 
	transaction_type, transaction_date 
	FROM
	(
		SELECT * FROM MON.SPARK_FT_MSC_TRANSACTION
		WHERE  
		(length(trim(fn_format_msisdn_to_9digits(other_party))) = 9 or served_msisdn like '1602%')
		AND (length(trim(fn_format_msisdn_to_9digits(served_msisdn))) = 9 or served_msisdn like '1601%' or served_msisdn like '1603%')
		AND other_party != served_msisdn 
		AND ( transaction_date BETWEEN date_sub(concat('###SLICE_VALUE###', '-01'), ${hivevar:nb_jour_30}) AND last_day(concat('###SLICE_VALUE###', '-01')) )
	)
)