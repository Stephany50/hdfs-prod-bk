SELECT  IF(T_1.REVENUE_EXISTS = 0
    AND T_2.SOURCE_DATA=5
    AND T_7.REVENUE_PRV_EXISTS >=1
    AND T_3.NB_INSERT=5
	AND ABS(T_4.VOICE_PAYGO-T_4.avg_VOICE_PAYGO) - 2*sqrt_VOICE_PAYGO < 0
	AND ABS(T_4.VOICE_BUNDLE-T_4.avg_VOICE_BUNDLE) - 2*sqrt_VOICE_BUNDLE < 0
	AND ABS(T_4.DATA_AMOUNT-T_4.avg_DATA_AMOUNT) - 2*sqrt_DATA_AMOUNT < 0
	AND ABS(T_6.ca_roaming_out-T_6.avg_ca_roaming_out) - 2*sqrt_ca_roaming_out < 0
	AND ABS(T_4.SMS_AMOUNT-T_4.avg_SMS_AMOUNT) - 2*sqrt_SMS_AMOUNT < 0
	AND ABS(T_4.CA_VAS_BRUT-T_4.avg_CA_VAS_BRUT) - 2*sqrt_CA_VAS_BRUT < 0
	AND ABS(T_5.total_amount-T_5.avg_total_amount) - 2*sqrt_total_amount < 0
,"OK","NOK") REVENUE_EXISTS
FROM
(
    select (nber_lines + nber_lines_backup) REVENUE_EXISTS
    from
    (SELECT COUNT(*) nber_lines FROM MON.SPARK_REVENUE_MARKETING WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_10,
    (SELECT COUNT(*) nber_lines_backup FROM MON.SPARK_REVENUE_MARKETING_back_up WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_11
) T_1,
(
    select (nber_lines_prev + nber_lines_prev_backup) REVENUE_PRV_EXISTS
    from
    (SELECT COUNT(*) nber_lines_prev FROM MON.SPARK_REVENUE_MARKETING WHERE TRANSACTION_DATE=date_sub('###SLICE_VALUE###',1)) T_70,
    (SELECT COUNT(*) nber_lines_prev_backup FROM MON.SPARK_REVENUE_MARKETING_back_up WHERE TRANSACTION_DATE=date_sub('###SLICE_VALUE###',1)) T_71
) T_7,
(SELECT COUNT(DISTINCT SOURCE_DATA) SOURCE_DATA  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE' AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT','FT_OVERDRAFT','IT_ZTE_ADJUSTMENT')) T_2,
(SELECT COUNT(DISTINCT INSERT_DATE) NB_INSERT  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE' AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT','FT_OVERDRAFT','IT_ZTE_ADJUSTMENT')) T_3,
(
	select
		sum(case when transaction_date =  '###SLICE_VALUE###' then VOICE_PAYGO else 0 end) VOICE_PAYGO,
		sum(case when transaction_date =  '###SLICE_VALUE###' then VOICE_BUNDLE else 0 end) VOICE_BUNDLE,
		sum(case when transaction_date =  '###SLICE_VALUE###' then SMS_AMOUNT else 0 end) SMS_AMOUNT,
		sum(case when transaction_date =  '###SLICE_VALUE###' then DATA_AMOUNT else 0 end) DATA_AMOUNT,
		sum(case when transaction_date =  '###SLICE_VALUE###' then CA_VAS_BRUT else 0 end) CA_VAS_BRUT,
		avg(VOICE_PAYGO) avg_VOICE_PAYGO,
		avg(VOICE_BUNDLE) avg_VOICE_BUNDLE,
		avg(SMS_AMOUNT) avg_SMS_AMOUNT,
		avg(DATA_AMOUNT) avg_DATA_AMOUNT,
		avg(CA_VAS_BRUT) avg_CA_VAS_BRUT,
		sqrt(variance(VOICE_PAYGO)) sqrt_VOICE_PAYGO,
		sqrt(variance(VOICE_BUNDLE)) sqrt_VOICE_BUNDLE,
		sqrt(variance(SMS_AMOUNT)) sqrt_SMS_AMOUNT,
		sqrt(variance(DATA_AMOUNT)) sqrt_DATA_AMOUNT,	
		sqrt(variance(CA_VAS_BRUT)) sqrt_CA_VAS_BRUT	
	from
	(

		select
			f.TRANSACTION_DATE transaction_date,
			SUM(case when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then nvl(TAXED_AMOUNT, 0) else 0 end) VOICE_PAYGO,
			sum(case when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then nvl(TAXED_AMOUNT, 0) else 0 end) VOICE_BUNDLE,
			sum(case when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT else 0 end) SMS_AMOUNT,
			sum(case when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then nvl(TAXED_AMOUNT, 0) else 0 end)  DATA_AMOUNT,
			sum(case when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT else 0 end) CA_VAS_BRUT
		FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
		WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE BETWEEN concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' 
		group by f.TRANSACTION_DATE

	) T
) T_4,
(
	select
		sum(case when transaction_date =  '###SLICE_VALUE###' then TOTAL_AMOUNT else 0 end) TOTAL_AMOUNT,
		avg(TOTAL_AMOUNT) avg_TOTAL_AMOUNT,
		sqrt(variance(TOTAL_AMOUNT)) sqrt_TOTAL_AMOUNT
	from
	(
		select
			e.transaction_date transaction_date,
			SUM(nvl(TAXED_AMOUNT, 0)) TOTAL_AMOUNT
		FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY e
		LEFT JOIN DIM.DT_OFFER_PROFILES ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
		WHERE TRAFFIC_MEAN='REVENUE'
			and e.OPERATOR_CODE  In  ('OCM')
			and SUB_ACCOUNT  In  ('MAIN')
			AND e.TRANSACTION_DATE  between concat(substr('###SLICE_VALUE###', 1, 7), '-01')  and '###SLICE_VALUE###' 
		group by e.transaction_date
	) T
) T_5,
(
	select
		sum(case when transaction_date =  '###SLICE_VALUE###' then ca_roaming_out else 0 end) ca_roaming_out,
		avg(ca_roaming_out) avg_ca_roaming_out,
		sqrt(variance(ca_roaming_out)) sqrt_ca_roaming_out
	from
	(
		select 
			transaction_date,
			sum(nvl(main_rated_amount, 0)) ca_roaming_out
		from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
		where transaction_date  between concat(substr('###SLICE_VALUE###', 1, 7), '-01')  and '###SLICE_VALUE###'  and destination like '%ROAM%'
		group by transaction_date
	) T
) T_6