INSERT INTO TMP.MSISDNS_INTERACTION2 
SELECT msisdn, other_msisdn, msisdn_rank
FROM 
(
	SELECT msisdn, other_msisdn,
	row_number() OVER(PARTITION BY msisdn ORDER BY nb_interaction DESC) AS msisdn_rank
	FROM
	(
		SELECT msisdn, other_msisdn, 
		SUM(
			CASE WHEN transaction_type in ('SMS_MT' ,'SMS_MO', 'SMS_MO_NOTIF') THEN 0.3 ELSE 0.7 END
		)  nb_interaction 
		FROM 
		(
			SELECT msisdn, other_msisdn, transaction_type 
			FROM TMP.MSISDNS_INTERACTION1 
		) Tab1 
		GROUP BY msisdn, other_msisdn 
	) Tab2  
) Tab3
WHERE msisdn_rank <= ${hivevar:nb_contact}