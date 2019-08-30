INSERT INTO MON.SMS_USAGE
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    transaction_date
FROM(
    SELECT *
    FROM  dim.dt_smsnotification_recipient
    WHERE type='SMSUSAGESMKT' AND actif='YES'
)A
LEFT JOIN (
  SELECT
        transaction_date,
        CONCAT(
        'En M ',DATE_FORMAT(a.TRANSACTION_DATE,'dd/MM')
        , ' \n' ,'-OnNet ',CAST(round(traffic_onnet/1000000,0) AS INT),'/',CAST(round(traffic_onnet_yd/1000000,0) AS INT)
        , ' \n' ,'-OffNet ',CAST(round(traffic_offnet/1000000,0) AS INT),'/',CAST(round(traffic_offnet_yd/1000000,0) AS INT)
        , ' \n' ,'-Inter ',round(traffic_inter/1000000,1),'/',round(traffic_inter_yd/1000000,1)
        , ' \n' ,'-SMS ',CAST(round(traffic_sms/1000000,0) AS INT),'/',CAST(round(traffic_sms_yd/1000000,0) AS INT)
        , ' \n' ,'-Voix ',CAST(round(traffic_voix/1000000,0) AS INT),'/',CAST(round(traffic_voix_yd/1000000,0) AS INT)
        , ', ' ,'MTD ',CAST(round(voix_mtd/1000000,0) AS INT)
        , ', ' ,'LMTD ',CAST(round(voix_lmtd/1000000,0) AS INT)
        , ', ' ,'%MoM ',round((voix_mtd/voix_lmtd -1)*100,1)
        , ' \n' ,'-Data(Go) ',CAST(round(traffic_data/1024/1024/1000000,0) AS INT),'/',CAST(round(traffic_data_yd/1024/1024/1000000,0) AS INT)
        , ', ' ,'MTD ',CAST(round(data_mtd/1024/1024/1000000,0) AS INT)
        , ', ' ,'LMTD ',CAST(round(data_lmtd/1024/1024/1000000,0) AS INT)
        , ', ' ,'%MoM ',round((data_mtd/data_lmtd -1)*100,1)) sms
        FROM(
            select
                '2019-08-28' transaction_date,
                sum(CASE WHEN transaction_date='2019-08-28' and destination = 'OUT_NAT_MOB_OCM' and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_onnet,
                sum(CASE WHEN transaction_date='2019-08-28' and destination IN ('OUT_NAT_MOB_CAM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_NEX') and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_offnet,
                sum(CASE WHEN transaction_date='2019-08-28' and trim(destination) in ('IN','OUT_INT') and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_inter,
                sum(CASE WHEN transaction_date='2019-08-28' and service_code = 'NVX_SMS' THEN TOTAL_COUNT
                    ELSE 0
                END) traffic_sms,
                sum(CASE WHEN transaction_date='2019-08-28' and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_voix,
                sum(CASE WHEN transaction_date=DATE_SUB('2019-08-28',1) and destination = 'OUT_NAT_MOB_OCM' and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_onnet_yd,
                sum(CASE WHEN transaction_date=DATE_SUB('2019-08-28',1) and destination IN ('OUT_NAT_MOB_CAM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_NEX') and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_offnet_yd,
                sum(CASE WHEN transaction_date=DATE_SUB('2019-08-28',1) and trim(destination) in ('IN','OUT_INT') and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_inter_yd,
                sum(CASE WHEN transaction_date=DATE_SUB('2019-08-28',1) and service_code = 'NVX_SMS' THEN TOTAL_COUNT
                    ELSE 0
                END) traffic_sms_yd,
                sum(CASE WHEN transaction_date=DATE_SUB('2019-08-28',1) and service_code = 'VOI_VOX' THEN DURATION
                    ELSE 0
                END) traffic_voix_yd
            from AGG.FT_GSM_TRAFFIC_REVENUE_DAILY
                where transaction_date between DATE_SUB('2019-08-28',1) and '2019-08-28'
        ) a join(
            select  '2019-08-28' sdate, sum(duration) voix_mtd
            from AGG.FT_GSM_TRAFFIC_REVENUE_DAILY
            where transaction_date BETWEEN CONCAT(SUBSTRING('2019-08-28',0,7),'-','01') and '2019-08-28'
            AND service_code = 'VOI_VOX'

        ) b on a.TRANSACTION_DATE = b.SDATE
        JOIN(
            select  '2019-08-28' SDATE, sum(duration)*CAST(SUBSTRING('2019-08-28',9,2) AS INT)/CAST(SUBSTRING(add_months('2019-08-28',-1),9,2) AS INT) voix_lmtd
            from AGG.FT_GSM_TRAFFIC_REVENUE_DAILY
            where transaction_date BETWEEN add_months(CONCAT(SUBSTRING('2019-08-28',0,7),'-','01'),-1)
                and add_months('2019-08-28',-1)
            AND service_code = 'VOI_VOX'

        ) c on b.SDATE = c.SDATE
        join(
            select
                '2019-08-28' datecode,
                sum(if(datecode='2019-08-28',BYTES_RECV+BYTES_SEND,0)) traffic_data,
                sum(if(datecode=DATE_SUB('2019-08-28',1),BYTES_RECV+BYTES_SEND,0)) traffic_data_yd
            from AGG.FT_A_gprs_activity
            where datecode between  DATE_SUB('2019-08-28',1) and '2019-08-28'
        ) d on c.SDATE = d.datecode
        join(
            select  '2019-08-28' datecode, sum(BYTES_RECV+BYTES_SEND) data_mtd
            from AGG.FT_A_gprs_activity
            where datecode BETWEEN CONCAT(SUBSTRING('2019-08-28',0,7),'-','01') and '2019-08-28'

        ) e on d.datecode = e.datecode
        join(
             select  '2019-08-28' datecode, sum(BYTES_RECV+BYTES_SEND)*CAST(SUBSTRING('2019-08-28',9,2) AS INT)/CAST(SUBSTRING(add_months('2019-08-28',-1),9,2) AS INT) data_lmtd
            from AGG.FT_A_gprs_activity
            where datecode BETWEEN add_months(CONCAT(SUBSTRING('2019-08-28',0,7),'-','01'),-1)
                and add_months('2019-08-28',-1)

        ) f on f.datecode = e.datecode

)B