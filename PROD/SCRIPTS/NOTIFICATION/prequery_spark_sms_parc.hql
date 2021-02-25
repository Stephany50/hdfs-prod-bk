SELECT MAX(REVENUE_EXISTS) REVENUE_EXISTS
FROM
(
SELECT  IF(T_1.SMS_EXISTS = 0 AND T_6.SMS_PREV_EXISTS>=1 AND T_2.FT_GROUP_SUBSCRIBER_SUMMARY_OK>=1 AND T_3.FT_GROUP_SUBSCRIBER_SUMMARY_PRE_OK >=1 AND T_4.FT_A_SUBSCRIBER_SUMMARY_OK >=1 AND T_5.FT_COMMERCIAL_SUBSCRIB_SUMMARY_OK >=1 
    AND ABS(parc_j/parc_j_prev - 1) <= 0.4 
    AND ABS(parc_pre/parc_pre_avant - 1) <= 0.4 
    AND ABS(res_pre/res_pre_prev - 1) <= 0.4 
    AND ABS(parc_pos/parc_pos_avant - 1) <= 0.4 
    AND ABS(parc_hyb/parc_hyb_prev - 1) <= 0.4 
    AND ABS(res_pos/res_pos_prev - 1) <= 0.4 
    AND ABS(new_pre/new_pre_prev - 1) <= 0.4 
    AND ABS(new_pos/new_pos_prev - 1) <= 0.4 
    AND new_mtd_perf.max_perf <= 0.4 
    AND lnew_mtd_perf.max_perf <= 0.4
,"OK","NOK") REVENUE_EXISTS
FROM
(SELECT COUNT(*) SMS_EXISTS FROM MON.SPARK_SMS_PARC WHERE SDATE=DATE_SUB('###SLICE_VALUE###',1)) T_1,
(SELECT COUNT(*) SMS_PREV_EXISTS FROM MON.SPARK_SMS_PARC WHERE SDATE=DATE_SUB('###SLICE_VALUE###',2)) T_6,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY_OK  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_GROUP_SUBSCRIBER_SUMMARY_PRE_OK  FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_3,
(SELECT COUNT(*) FT_A_SUBSCRIBER_SUMMARY_OK FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE DATECODE =DATE_SUB('###SLICE_VALUE###',1))T_4 ,
(SELECT COUNT(*) FT_COMMERCIAL_SUBSCRIB_SUMMARY_OK FROM MON.SPARK_FT_COMMERCIAL_SUBSCRIB_SUMMARY WHERE DATECODE = DATE_SUB('###SLICE_VALUE###',1)) T_5,
(
SELECT SUM (CASE WHEN event_date='###SLICE_VALUE###' THEN effectif ELSE 0 END) parc_j,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) THEN effectif ELSE 0 END) parc_j_prev,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_avant,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle = 'PURE PREPAID' THEN deconnexions ELSE 0 END) res_pre,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle = 'PURE PREPAID' THEN deconnexions ELSE 0 END) res_pre_prev,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_avant,
	   SUM (CASE WHEN event_date='###SLICE_VALUE###' and cust_billcycle = 'HYBRID' THEN effectif ELSE 0 END) parc_hyb,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle = 'HYBRID' THEN effectif ELSE 0 END) parc_hyb_prev,
	   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN deconnexions ELSE 0 END) res_pos,
	   SUM (CASE WHEN event_date=date_sub('###SLICE_VALUE###', 1) and cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN deconnexions ELSE 0 END) res_pos_prev
FROM MON.SPARK_FT_GROUP_SUBScriber_summary
WHERE operator_code <> 'SET'
AND (CASE
	  WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
		  1
	  ELSE 0
	END) = 0
AND event_date between date_sub('###SLICE_VALUE###', 1) and '###SLICE_VALUE###'
) T_7,
(
SELECT   
    SUM (CASE WHEN datecode=date_sub('###SLICE_VALUE###', 1) and network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') = 'PURE PREPAID' THEN total_activation ELSE 0 END ) new_pre,
	SUM (CASE WHEN datecode=date_sub('###SLICE_VALUE###', 2) and network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') = 'PURE PREPAID' THEN total_activation ELSE 0 END ) new_pre_prev,
    SUM (CASE WHEN datecode=date_sub('###SLICE_VALUE###', 1) and network_domain = 'GSM' AND account_status = 'ACTIF'AND subscriber_type IN ('HYBRID', 'PURE POSTPAID') THEN total_activation ELSE 0 END ) new_pos, 
	SUM (CASE WHEN datecode=date_sub('###SLICE_VALUE###', 2) and network_domain = 'GSM' AND account_status = 'ACTIF'AND subscriber_type IN ('HYBRID', 'PURE POSTPAID') THEN total_activation ELSE 0 END ) new_pos_prev
FROM AGG.SPARK_FT_a_subscriber_summary e
WHERE account_status  IN ('ACTIF', 'INACT')
AND commercial_offer NOT LIKE 'PREPAID SET%'
AND datecode between date_sub('###SLICE_VALUE###', 2) and date_sub('###SLICE_VALUE###', 1)
) T_8,
(
    SELECT max(case when datecode =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(new_mtd/new_mtd_prev-1) end ) max_perf 
from (
    select 
        datecode, 
        new_mtd, 
        LAG(new_mtd) OVER (PARTITION BY id ORDER BY datecode ) new_mtd_prev 
    from  (select datecode, SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') IN ('HYBRID', 'PURE POSTPAID','PURE PREPAID')  THEN total_activation ELSE 0 END ) new_mtd, 1 id  
    from AGG.SPARK_FT_a_subscriber_summary a  
    where account_status  IN ('ACTIF', 'INACT')
	   AND commercial_offer NOT LIKE 'PREPAID SET%' and 
	   datecode between  last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###' 
    group by datecode)a 
    ) d  group by datecode
) new_mtd_perf 
,(
    SELECT max(case when datecode =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(new_lmtd/new_lmtd_prev-1) end ) max_perf 
    from (
        select 
        datecode, 
        new_lmtd, 
        LAG(new_lmtd) OVER (PARTITION BY id ORDER BY datecode ) new_lmtd_prev 
    from  (
        select datecode, SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') IN ('HYBRID', 'PURE POSTPAID','PURE PREPAID') THEN total_activation ELSE 0 END ) new_lmtd, 1 id  
        from AGG.SPARK_FT_a_subscriber_summary a  
        where account_status  IN ('ACTIF', 'INACT')
			AND commercial_offer NOT LIKE 'PREPAID SET%' 
			and datecode between  last_day(add_months('###SLICE_VALUE###',-2))  and add_months('###SLICE_VALUE###',-1) 
        group by datecode
     )a 
    ) d  group by datecode
) lnew_mtd_perf
) A