SELECT  IF(T_1.REVENUE_EXISTS = 0 AND T_2.SOURCE_DATA=5 AND T_7.REVENUE_PRV_EXISTS >=1 AND T_3.NB_INSERT=5 
    AND ABS(T_4.VOICE_PAYGO/T_4.VOICE_PAYGO_prev-1)<=0.1 
    AND ABS(T_4.VOICE_BUNDLE/T_4.VOICE_BUNDLE_prev-1)<=0.1 
    AND ABS(T_4.DATA_AMOUNT/T_4.DATA_AMOUNT_prev-1)<=0.1 
    AND ABS(T_6.ca_roaming_out/T_6.ca_roaming_out_yd-1)<=0.1  
    AND ABS(T_4.SMS_AMOUNT/T_4.SMS_AMOUNT_prev-1)<=0.1  
    AND ABS(T_4.CA_VAS_BRUT/T_4.CA_VAS_BRUT_prev-1)<=0.3  
    AND ABS(T_5.total_amount/T_5.TOTAL_AMOUNT_yd-1)<=0.1  
    AND mtd_perf.max_perf <=0.4
    AND lmtd_perf.max_perf<=0.4  
    AND T_8.nb_source_data = T_8.nb_insert_date
,"OK","NOK") REVENUE_EXISTS
FROM
(SELECT COUNT(*) REVENUE_EXISTS FROM MON.SPARK_REVENUE_MARKETING WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) REVENUE_PRV_EXISTS FROM MON.SPARK_REVENUE_MARKETING WHERE TRANSACTION_DATE=date_sub('###SLICE_VALUE###',1)) T_7,
(SELECT COUNT(DISTINCT SOURCE_DATA) SOURCE_DATA  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE' AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT','FT_OVERDRAFT','IT_ZTE_ADJUSTMENT')) T_2,
(SELECT COUNT(DISTINCT INSERT_DATE) NB_INSERT  FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND TRAFFIC_MEAN='REVENUE' AND SOURCE_DATA NOT IN ('FT_SUBS_RETAIL_ZEBRA','FT_A_DATA_TRANSFER','FT_CONTRACT_SNAPSHOT','FT_OVERDRAFT','IT_ZTE_ADJUSTMENT')) T_3,
(
    select
SUM(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then TAXED_AMOUNT
	else 0
end) VOICE_PAYGO,
SUM(case
	when transaction_date = DATE_SUB('###SLICE_VALUE###', 1) and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then TAXED_AMOUNT
	else 0
end) VOICE_PAYGO_prev,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then TAXED_AMOUNT
	else 0
end) VOICE_BUNDLE,
sum(case
	when transaction_date = DATE_SUB('###SLICE_VALUE###', 1) and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then TAXED_AMOUNT
	else 0
end) VOICE_BUNDLE_prev,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT
	else 0
end) SMS_AMOUNT,
sum(case
	when transaction_date = DATE_SUB('###SLICE_VALUE###', 1) and service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT
	else 0
end) SMS_AMOUNT_prev,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then TAXED_AMOUNT
	else 0
end)  DATA_AMOUNT,
sum(case
	when transaction_date = DATE_SUB('###SLICE_VALUE###', 1) and service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then TAXED_AMOUNT
	else 0
end)  DATA_AMOUNT_prev,
sum(case
	when transaction_date = '###SLICE_VALUE###' and service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT
	else 0
end) CA_VAS_BRUT,
sum(case
	when transaction_date = DATE_SUB('###SLICE_VALUE###', 1) and service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT
	else 0
end) CA_VAS_BRUT_prev
FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 1) AND '###SLICE_VALUE###'
) T_4,
(
select
    SUM(case when transaction_date = '###SLICE_VALUE###' then TAXED_AMOUNT
            else 0
        end) TOTAL_AMOUNT,
    SUM(case when transaction_date = DATE_SUB('###SLICE_VALUE###',1)  then TAXED_AMOUNT
            else 0
        end) TOTAL_AMOUNT_yd
FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY e
LEFT JOIN DIM.DT_OFFER_PROFILES ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
WHERE TRAFFIC_MEAN='REVENUE'
    and e.OPERATOR_CODE  In  ('OCM')
    and SUB_ACCOUNT  In  ('MAIN')
    AND e.TRANSACTION_DATE  between DATE_SUB('###SLICE_VALUE###',1) and '###SLICE_VALUE###'
) T_5,
(
select max(transaction_date) roaming_date,
    sum(case when transaction_date = '###SLICE_VALUE###' then main_rated_amount
        else 0
    end) ca_roaming_out,
    sum(case when transaction_date = DATE_SUB('###SLICE_VALUE###',1) then main_rated_amount
        else 0
    end) ca_roaming_out_yd
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date   between DATE_SUB('###SLICE_VALUE###',1) and '###SLICE_VALUE###' and destination like '%ROAM%'
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
    ) d  group by transaction_date
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
) d  group by transaction_date
) lmtd_perf ,
(
select count(distinct source_data) nb_source_data, count(distinct insert_date) nb_insert_date
from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY where TRANSACTION_DATE='###SLICE_VALUE###'
) T_8