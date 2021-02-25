SELECT IF(T_1.SMS_EXISTS = 0 AND T_5.SMS_PREV_EXISTS > 0 AND T_2.GSM_TRAFFIC_EXISTS>0 AND T_3.FT_A_GPRS_ACTIVITY_EXISTS>0 
    AND ABS(T_6.traffic_onnet/T_6.traffic_onnet_yd-1)<=0.4 
    AND ABS(T_6.traffic_offnet/T_6.traffic_offnet_yd-1)<=0.4 
    AND ABS(T_6.traffic_inter/T_6.traffic_inter_yd-1)<=0.4
    AND ABS(T_6.traffic_sms/T_6.traffic_sms_yd-1)<=0.4 
    AND ABS(T_6.traffic_voix/T_6.traffic_voix_yd-1)<=0.4
    AND ABS(T_7.traffic_data/T_7.traffic_data_yd-1)<=0.4 
    AND data_mtd_perf.max_perf<=0.4 
    AND ldata_mtd_perf.max_perf<=0.4 
    AND voix_mtd_perf.max_perf<=0.4 
    AND lvoix_mtd_perf.max_perf<=0.4 
,"OK","NOK") REVENUE_EXISTS
FROM
(SELECT COUNT(*) SMS_EXISTS FROM MON.SPARK_SMS_USAGE WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) SMS_PREV_EXISTS FROM MON.SPARK_SMS_USAGE WHERE TRANSACTION_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_5,
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
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and trim(destination) in ('IN','OUT_INT') and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_inter,
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and service_code = 'NVX_SMS' THEN TOTAL_COUNT
        ELSE 0
    END) traffic_sms,
    sum(CASE WHEN transaction_date='###SLICE_VALUE###' and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_voix,
    sum(CASE WHEN transaction_date=DATE_SUB('###SLICE_VALUE###',1) and destination = 'OUT_NAT_MOB_OCM' and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_onnet_yd,
    sum(CASE WHEN transaction_date=DATE_SUB('###SLICE_VALUE###',1) and destination IN ('OUT_NAT_MOB_CAM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_NEX') and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_offnet_yd,
    sum(CASE WHEN transaction_date=DATE_SUB('###SLICE_VALUE###',1) and trim(destination) in ('IN','OUT_INT') and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_inter_yd,
    sum(CASE WHEN transaction_date=DATE_SUB('###SLICE_VALUE###',1) and service_code = 'NVX_SMS' THEN TOTAL_COUNT
        ELSE 0
    END) traffic_sms_yd,
    sum(CASE WHEN transaction_date=DATE_SUB('###SLICE_VALUE###',1) and service_code = 'VOI_VOX' THEN DURATION
        ELSE 0
    END) traffic_voix_yd
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
    where transaction_date between DATE_SUB('###SLICE_VALUE###',1) and '###SLICE_VALUE###'
) T_6,
(
select
    sum(if(datecode='###SLICE_VALUE###',BYTES_RECV+BYTES_SEND,0)) traffic_data,
    sum(if(datecode=DATE_SUB('###SLICE_VALUE###',1),BYTES_RECV+BYTES_SEND,0)) traffic_data_yd
from AGG.SPARK_FT_A_gprs_activity
where datecode between  DATE_SUB('###SLICE_VALUE###',1) and '###SLICE_VALUE###'
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
        select datecode, sum(BYTES_RECV+BYTES_SEND) data_mtd, 1 id  
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