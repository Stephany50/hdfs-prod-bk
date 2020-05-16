INSERT INTO MON.SPARK_SMS_USAGE
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    transaction_date
FROM(
    SELECT *
    FROM  dim.spark_dt_smsnotification_recipient
    WHERE type='SMSUSAGESMKT' AND actif='YES'
)A
LEFT JOIN (
  SELECT
        transaction_date,
        CONCAT(
        DATE_FORMAT(a.TRANSACTION_DATE,'dd/MM')
        , ' \n' ,'En M '
        , ' \n' ,'-OnNet ',CAST(round(traffic_onnet/1000000,0) AS INT),'/',CAST(round(traffic_onnet_yd/1000000,0) AS INT)
        , ' \n' ,'-OffNet ',CAST(round(traffic_offnet/1000000,0) AS INT),'/',CAST(round(traffic_offnet_yd/1000000,0) AS INT)
        , ' \n' ,'-Inter ',round(traffic_inter/1000000,1),'/',round(traffic_inter_yd/1000000,1)
        , ' \n' ,'-SMS ',CAST(round(traffic_sms/1000000,0) AS INT),'/',CAST(round(traffic_sms_yd/1000000,0) AS INT)
        , ' \n' ,'-Voix ',CAST(round(traffic_voix/1000000,0) AS INT),'/',CAST(round(traffic_voix_yd/1000000,0) AS INT)
        , ', ' ,'MTD ',CAST(round(voix_mtd/1000000,0) AS INT)
        , ', ' ,'LMTD ',CAST(round(voix_lmtd/1000000,0) AS INT)
        , ', ' ,'%MoM ',round((voix_mtd/voix_lmtd -1)*100,1)
        , ' \n' ,'En To: Data ',CAST(round(traffic_data/1024/1024/1000000,0) AS INT),'/',CAST(round(traffic_data_yd/1024/1024/1000000,0) AS INT)
        , ', ' ,'MTD ',CAST(round(data_mtd/1024/1024/1000000,0) AS INT)
        , ', ' ,'LMTD ',CAST(round(data_lmtd/1024/1024/1000000,0) AS INT)
        , ', ' ,'%MoM ',round((data_mtd/data_lmtd -1)*100,1)) sms
        FROM(
            select
                '###SLICE_VALUE###' transaction_date,
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
        ) a join(
            select  '###SLICE_VALUE###' sdate, sum(duration) voix_mtd
            from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
            where transaction_date BETWEEN CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
            AND service_code = 'VOI_VOX'

        ) b on a.TRANSACTION_DATE = b.SDATE
        JOIN(
            select  '###SLICE_VALUE###' SDATE, sum(duration)*CAST(SUBSTRING('###SLICE_VALUE###',9,2) AS INT)/CAST(SUBSTRING(add_months('###SLICE_VALUE###',-1),9,2) AS INT) voix_lmtd
            from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
            where transaction_date BETWEEN add_months(CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01'),-1)
                and add_months('###SLICE_VALUE###',-1)
            AND service_code = 'VOI_VOX'

        ) c on b.SDATE = c.SDATE
        join(
            select
                '###SLICE_VALUE###' datecode,
                sum(if(datecode='###SLICE_VALUE###',BYTES_RECV+BYTES_SEND,0)) traffic_data,
                sum(if(datecode=DATE_SUB('###SLICE_VALUE###',1),BYTES_RECV+BYTES_SEND,0)) traffic_data_yd
            from AGG.SPARK_FT_A_gprs_activity
            where datecode between  DATE_SUB('###SLICE_VALUE###',1) and '###SLICE_VALUE###'
        ) d on c.SDATE = d.datecode
        join(
            select  '###SLICE_VALUE###' datecode, sum(BYTES_RECV+BYTES_SEND) data_mtd
            from AGG.SPARK_FT_A_gprs_activity
            where datecode BETWEEN CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'

        ) e on d.datecode = e.datecode
        join(
             select  '###SLICE_VALUE###' datecode, sum(BYTES_RECV+BYTES_SEND)*CAST(SUBSTRING('###SLICE_VALUE###',9,2) AS INT)/CAST(SUBSTRING(add_months('###SLICE_VALUE###',-1),9,2) AS INT) data_lmtd
            from AGG.SPARK_FT_A_gprs_activity
            where datecode BETWEEN add_months(CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01'),-1)
                and add_months('###SLICE_VALUE###',-1)

        ) f on f.datecode = e.datecode

)B