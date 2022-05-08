SELECT  
IF(T_1.REVENUE_EXISTS = 0
    AND T_2.SOURCE_DATA=10
    AND T_7.REVENUE_PRV_EXISTS >=1
    AND T_3.NB_INSERT=10
    AND ABS(T_4.VOICE_PAYGO/T_4.VOICE_PAYGO_avg-1)<0.5
    AND ABS(T_4.VOICE_BUNDLE/T_4.VOICE_BUNDLE_avg-1)<0.5 
    AND ABS(T_4.DATA_AMOUNT/T_4.DATA_AMOUNT_avg-1)<0.5
    AND ABS(T_6.ca_roaming_out/T_6.ca_roaming_out_avg-1)<0.5
    AND ABS(T_4.SMS_AMOUNT/T_4.SMS_AMOUNT_avg-1)<0.5
    AND ABS(T_4.CA_VAS_BRUT/T_4.CA_VAS_BRUT_avg-1)<0.5  
    AND ABS(T_5.total_amount/T_5.TOTAL_AMOUNT_avg-1)<0.5  
    AND mtd_perf.max_perf <0.5
    AND lmtd_perf.max_perf<0.5  
    AND T_8.nb_source_data = T_8.nb_insert_date
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
(SELECT COUNT(DISTINCT SOURCE_DATA) SOURCE_DATA  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE') T_2, -- IT_ZTE_ADJUSTMENT FT_OVERDRAFT AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT')
(SELECT COUNT(DISTINCT INSERT_DATE) NB_INSERT  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE') T_3, -- IT_ZTE_ADJUSTMENT FT_OVERDRAFT AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT')
(
    select
SUM(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then TAXED_AMOUNT
	else 0
end) VOICE_PAYGO,
SUM(case
	when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then TAXED_AMOUNT
	else 0
end)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) VOICE_PAYGO_avg,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then TAXED_AMOUNT
	else 0
end) VOICE_BUNDLE,
sum(case
	when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then TAXED_AMOUNT
	else 0
end)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) VOICE_BUNDLE_avg,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT
	else 0
end) SMS_AMOUNT,
sum(case
	when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT
	else 0
end)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) SMS_AMOUNT_avg,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then TAXED_AMOUNT
	else 0
end)  DATA_AMOUNT,
sum(case
	when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then TAXED_AMOUNT
	else 0
end)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1)  DATA_AMOUNT_avg,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT
	else 0
end) CA_VAS_BRUT,
sum(case
	when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT
	else 0
end)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) CA_VAS_BRUT_avg
FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE BETWEEN last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###' 
) T_4,
(
select
    SUM(case when transaction_date = '###SLICE_VALUE###' then TAXED_AMOUNT
            else 0
        end) TOTAL_AMOUNT,
    SUM(TAXED_AMOUNT)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) TOTAL_AMOUNT_avg
FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY e
LEFT JOIN DIM.DT_OFFER_PROFILES ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
WHERE TRAFFIC_MEAN='REVENUE'
    and e.OPERATOR_CODE  In  ('OCM')
    and SUB_ACCOUNT  In  ('MAIN')
    AND e.TRANSACTION_DATE  between last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###' 
) T_5,
(
select 
    sum(case when transaction_date = '###SLICE_VALUE###' then main_rated_amount
        else 0
    end) ca_roaming_out,
    sum(main_rated_amount)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) ca_roaming_out_avg
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date   between last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###'  and destination like '%ROAM%'
) T_6,
(
SELECT max(case when transaction_date =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(taxed_amount/taxed_amount_prev-1) end ) max_perf 
from (
    select 
        transaction_date, 
        taxed_amount, 
        LAG(taxed_amount) OVER (PARTITION BY id ORDER BY transaction_date ) taxed_amount_prev 
    from  (select transaction_date, sum(TAXED_AMOUNT) TAXED_AMOUNT, 1 id  
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY a  
    where TRAFFIC_MEAN='REVENUE' and a.OPERATOR_CODE  In  ('OCM')  and SUB_ACCOUNT  In  ('MAIN') AND TRANSACTION_DATE between  last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###' 
    group by transaction_date)a 
    ) d 
) mtd_perf,
(
SELECT max(case when transaction_date =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(taxed_amount/taxed_amount_prev-1) end ) max_perf 
from (
	select 
	transaction_date, 
	taxed_amount, 
	LAG(taxed_amount) OVER (PARTITION BY id ORDER BY transaction_date ) taxed_amount_prev 
from  (
	select transaction_date, sum(TAXED_AMOUNT) TAXED_AMOUNT, 1 id  
	from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY a  
	where TRAFFIC_MEAN='REVENUE' and a.OPERATOR_CODE  In  ('OCM')  and SUB_ACCOUNT  In  ('MAIN') AND TRANSACTION_DATE between  last_day(add_months('###SLICE_VALUE###',-2))  and add_months('###SLICE_VALUE###',-1)
	group by transaction_date
 )a 
) d  
) lmtd_perf ,
(
select count(distinct source_data) nb_source_data, count(distinct insert_date) nb_insert_date
from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY where TRANSACTION_DATE='###SLICE_VALUE###'
) T_8
