SELECT IF(T_1.SMS_EXISTS = 0 
    AND T_5.SMS_PREV_EXISTS > 0 
    AND T_2.GSM_TRAFFIC_EXISTS>0 
    AND T_3.FT_A_GPRS_ACTIVITY_EXISTS>0 
    AND ABS(T_6.traffic_onnet/T_6.avg_traffic_onnet_mtd-1)<=0.1 
    AND ABS(T_6.traffic_offnet/T_6.avg_traffic_offnet_mtd-1)<=0.1 
    AND ABS(T_6.traffic_sms/T_6.avg_traffic_sms_mtd-1)<=0.1 
    AND ABS(T_6.traffic_voix/T_6.avg_traffic_voix_mtd-1)<=0.1
    AND ABS(T_7.traffic_data/T_7.avg_traffic_data_mtd-1)<=0.1 
    AND data_mtd_perf.max_perf<=0.4 
    AND ldata_mtd_perf.max_perf<=0.4 
    AND voix_mtd_perf.max_perf<=0.4 
    AND lvoix_mtd_perf.max_perf<=0.4 
,"OK","NOK") REVENUE_EXISTS
FROM
(
    select (nber_lines + nber_lines_backup) SMS_EXISTS
    from
    (SELECT COUNT(*) nber_lines FROM MON.SPARK_SMS_USAGE WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_50,
    (SELECT COUNT(*) nber_lines_backup FROM MON.SPARK_SMS_USAGE_backup WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_51
) T_1,
(
    select (nber_lines_prev + nber_lines_prev_backup) SMS_PREV_EXISTS
    from
    (SELECT COUNT(*) nber_lines_prev FROM MON.SPARK_SMS_USAGE WHERE TRANSACTION_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_50,
    (SELECT COUNT(*) nber_lines_prev_backup FROM MON.SPARK_SMS_USAGE_backup WHERE TRANSACTION_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_51
) T_5,
(SELECT COUNT(*) GSM_TRAFFIC_EXISTS  FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_A_GPRS_ACTIVITY_EXISTS  FROM AGG.SPARK_FT_A_GPRS_ACTIVITY WHERE DATECODE='###SLICE_VALUE###') T_3,
(
select
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and destination = 'OUT_NAT_MOB_OCM' and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_onnet,
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and destination IN ('OUT_NAT_MOB_CAM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_NEX') and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_offnet,
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and service_code = 'NVX_SMS' THEN TOTAL_COUNT
        ELSE 0
    END) traffic_sms,
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_voix,
    sum(CASE WHEN destination = 'OUT_NAT_MOB_OCM' and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) avg_traffic_onnet_mtd,
    sum(CASE WHEN destination IN ('OUT_NAT_MOB_CAM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_NEX') and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) avg_traffic_offnet_mtd,
    sum(CASE WHEN service_code = 'NVX_SMS' THEN TOTAL_COUNT
        ELSE 0
    END)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) avg_traffic_sms_mtd,
    sum(CASE WHEN service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END)/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) avg_traffic_voix_mtd
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
    where transaction_date between last_day(add_months('###SLICE_VALUE###',-1)) and '###SLICE_VALUE###'
) T_6,
(
select
    sum(if(datecode='###SLICE_VALUE###',nvl(BYTES_RECV+BYTES_SEND, 0),0)) traffic_data,
    sum(nvl(BYTES_RECV+BYTES_SEND,0))/(datediff('###SLICE_VALUE###', last_day(add_months('###SLICE_VALUE###',-1))) + 1) avg_traffic_data_mtd
from AGG.SPARK_FT_A_gprs_activity
where datecode between  last_day(add_months('###SLICE_VALUE###',-1)) and '###SLICE_VALUE###'
) T_7,
(
    SELECT max(case when datecode =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(data_mtd/data_mtd_prev-1) end ) max_perf 
from (
    select 
        datecode, 
        data_mtd, 
        LAG(data_mtd) OVER (PARTITION BY id ORDER BY datecode ) data_mtd_prev 
    from  (select datecode, sum(BYTES_RECV+BYTES_SEND) data_mtd, 1 id  
    from AGG.SPARK_FT_A_gprs_activity a  
    where datecode between  last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###' 
    group by datecode)a 
    ) d  
) data_mtd_perf 
,(
    SELECT max(case when datecode =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(data_mtd/data_mtd_prev-1) end ) max_perf 
    from (
        select 
        datecode, 
        data_mtd, 
        LAG(data_mtd) OVER (PARTITION BY id ORDER BY datecode ) data_mtd_prev 
    from  (
        select datecode, sum(nvl(BYTES_RECV+BYTES_SEND, 0)) data_mtd, 1 id  
        from AGG.SPARK_FT_A_gprs_activity a  
        where datecode between  last_day(add_months('###SLICE_VALUE###',-2))  and add_months('###SLICE_VALUE###',-1)
        group by datecode
     )a 
    ) d  
) ldata_mtd_perf ,
(
    SELECT max(case when transaction_date =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(voix_mtd/voix_mtd_prev-1) end ) max_perf 
from (
    select 
        transaction_date, 
        voix_mtd, 
        LAG(voix_mtd) OVER (PARTITION BY id ORDER BY transaction_date ) voix_mtd_prev 
    from  (select transaction_date, sum(duration) voix_mtd, 1 id  
    from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY a  
    where transaction_date between  last_day(add_months('###SLICE_VALUE###',-1))  and '###SLICE_VALUE###' AND service_code = 'VOI_VOX'
    group by transaction_date)a 
    ) d 
) voix_mtd_perf 
,(
    SELECT max(case when transaction_date =last_day(add_months('###SLICE_VALUE###',-1)) then 0 else  abs(voix_mtd/voix_mtd_prev-1) end ) max_perf 
    from (
        select 
        transaction_date, 
        voix_mtd, 
        LAG(voix_mtd) OVER (PARTITION BY id ORDER BY transaction_date ) voix_mtd_prev 
    from  (
        select transaction_date, sum(duration) voix_mtd, 1 id  
        from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY a  
        where transaction_date between  last_day(add_months('###SLICE_VALUE###',-2))  and add_months('###SLICE_VALUE###',-1) AND service_code = 'VOI_VOX'
        group by transaction_date
     )a 
    ) d 
) lvoix_mtd_perf 