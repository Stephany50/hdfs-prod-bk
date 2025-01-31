SELECT  IF(T_1.SMS_EXISTS = 0 
	AND T_6.SMS_PREV_EXISTS>=1 
	AND T_2.FT_GROUP_SUBSCRIBER_SUMMARY_OK>=1 
	AND T_3.FT_GROUP_SUBSCRIBER_SUMMARY_PRE_OK >=1 
	AND T_4.FT_A_SUBSCRIBER_SUMMARY_OK >=1 
	AND T_5.FT_COMMERCIAL_SUBSCRIB_SUMMARY_OK >=1 
    AND ABS(parc_j/parc_j_prev - 1) <0.5
    AND ABS(parc_pre/parc_pre_avant - 1) <0.5 
    AND ABS(parc_pos/parc_pos_avant - 1) <0.5
    AND ABS(parc_hyb/parc_hyb_prev - 1) <0.5
,"OK","NOK") REVENUE_EXISTS
FROM
(
	select (nber_lines + nber_lines_backup) SMS_EXISTS
	from
	(SELECT COUNT(*) nber_lines FROM MON.SPARK_SMS_PARC WHERE SDATE=DATE_SUB('###SLICE_VALUE###',1)) T_10,
	(SELECT COUNT(*) nber_lines_backup FROM MON.SPARK_SMS_PARC_backup WHERE SDATE=DATE_SUB('###SLICE_VALUE###',1)) T_11
) T_1,
(
	select (nber_lines_prev + nber_lines_prev_backup) SMS_PREV_EXISTS
	from
	(SELECT COUNT(*) nber_lines_prev FROM MON.SPARK_SMS_PARC WHERE SDATE=DATE_SUB('###SLICE_VALUE###',2)) T_60,
	(SELECT COUNT(*) nber_lines_prev_backup FROM MON.SPARK_SMS_PARC_backup WHERE SDATE=DATE_SUB('###SLICE_VALUE###',2)) T_61
) T_6,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY_OK  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY_PRE_OK  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_3,
(SELECT COUNT(*) FT_A_SUBSCRIBER_SUMMARY_OK FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE DATECODE =DATE_SUB('###SLICE_VALUE###',1))T_4 ,
(SELECT COUNT(*) FT_COMMERCIAL_SUBSCRIB_SUMMARY_OK FROM MON.SPARK_FT_COMMERCIAL_SUBSCRIB_SUMMARY WHERE DATECODE = DATE_SUB('###SLICE_VALUE###',1)) T_5,
(
SELECT SUM (CASE WHEN event_date='###SLICE_VALUE###' THEN effectif ELSE 0 END) parc_j,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) THEN effectif ELSE 0 END) parc_j_prev,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_avant,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_avant,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle = 'HYBRID' THEN effectif ELSE 0 END) parc_hyb,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle = 'HYBRID' THEN effectif ELSE 0 END) parc_hyb_prev
FROM MON.SPARK_FT_GROUP_SUBScriber_summary
WHERE operator_code <> 'SET'
AND (CASE
	  WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
		  1
	  ELSE 0
	END) = 0
AND event_date between date_sub('###SLICE_VALUE###', 1) and '###SLICE_VALUE###'
) T_7