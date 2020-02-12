
INSERT INTO MON.SPARK_REVENUE_MARKETING
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    TRANSACTION_DATE
FROM(
    SELECT *
    FROM  dim.spark_dt_smsnotification_recipient
    WHERE type='SMSREVENUMKT' AND actif='YES'
)A
LEFT JOIN (
    select
        a.event_date TRANSACTION_DATE,
        CONCAT(
            DATE_FORMAT(a.TRANSACTION_DATE,'dd/MM')
            ,' \n' ,'-Vx Pgo',round(voice_paygo/1000000,1), '/',round(voice_paygo_yd/1000000,1)
            ,' \n' ,'-Bdl ',round(voice_bundle/1000000,1) , '/', round(voice_bundle_yd/1000000,1)
            ,' \n' ,'-SMS ',round(sms_amount/1000000,1) , '/',round(sms_amount_yd/1000000,1)
            ,' \n' ,'-Data ',round(data_amount/1000000,1) , '/',round(data_amount_yd/1000000,1)
            ,' \n' ,'-Roam ',round(ca_roaming_out/1000000,1) , '/',round(ca_roaming_out_yd/1000000,1)
            ,' \n' ,'-Vas ',round(ca_vas_brut/1000000,1), '/',round(ca_vas_brut_yd/1000000,1)
            ,' \n' ,'-Total ',round(total_amount/1000000,1)
            ,' \n' ,'-MTD ',round(mtd_amount/1000000,1)
            ,' \n' ,'-LMTD ',round(lmtd_amount /1000000,1)
            ,' \n' ,'-% ',round((mtd_amount - lmtd_amount)*100/lmtd_amount,1)) sms
        from (
            SELECT * FROM (
                 select
                    '2020-01-29' event_date,
                    SUM(case
                        when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then TAXED_AMOUNT
                        else 0
                    end) VOICE_PAYGO,
                    sum(case
                        when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then TAXED_AMOUNT
                        else 0
                    end) VOICE_BUNDLE,
                    sum(case
                        when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT
                        else 0
                    end) SMS_AMOUNT,
                    sum(case
                        when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then TAXED_AMOUNT
                        else 0
                    end)  DATA_AMOUNT,
                    sum(case
                        when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT
                        else 0
                    end) CA_VAS_BRUT
                FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
                WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE = '2020-01-29'
            )T1
            LEFT JOIN (
                SELECT '2020-01-29' TRANSACTION_DATE,
                SUM(case
                    when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'VOIX'  then TAXED_AMOUNT
                    else 0
                end) VOICE_PAYGO_yd,
                sum(case
                    when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) = 'BUNDLE VOIX'  then TAXED_AMOUNT
                    else 0
                end) VOICE_BUNDLE_yd,
                sum(case
                    when service_code = usage_code AND UPPER(USAGE_DESCRIPTION) IN ('SMS','BUNDLE SMS')  then TAXED_AMOUNT
                    else 0
                end) SMS_AMOUNT_yd,
                sum(case
                    when service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_USS', 'NVX_GPRS_PAYGO')  then TAXED_AMOUNT
                    else 0
                end)  DATA_AMOUNT_yd,
                sum(case
                    when  service_code = usage_code AND UPPER(USAGE_CODE) in ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO') then TAXED_AMOUNT
                    else 0
                end) CA_VAS_BRUT_yd
                FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
                WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE   = DATE_SUB('2020-01-29',1)
            )T2 ON T1.event_date=T2.TRANSACTION_DATE

        ) a
        join (

            select max(transaction_date) roaming_date,
                sum(case when transaction_date = '2020-01-29' then main_rated_amount
                    else 0
                end) ca_roaming_out,
                sum(case when transaction_date = DATE_SUB('2020-01-29',1) then main_rated_amount
                    else 0
                end) ca_roaming_out_yd
            from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
            where transaction_date   between DATE_SUB('2020-01-29',1) and '2020-01-29' and destination like '%ROAM%'
        ) b on b.roaming_date = a.event_date
        join (
            select
            max(e.TRANSACTION_DATE) transaction_date,
            SUM(case when transaction_date = '2020-01-29' then TAXED_AMOUNT
                    else 0
                end) TOTAL_AMOUNT,
            SUM(case when transaction_date = DATE_SUB('2020-01-29',1)  then TAXED_AMOUNT
                    else 0
                end) TOTAL_AMOUNT_yd
            FROM  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY e
            LEFT JOIN DIM.DT_OFFER_PROFILES ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
            WHERE TRAFFIC_MEAN='REVENUE'
                and e.OPERATOR_CODE  In  ('OCM')
                and SUB_ACCOUNT  In  ('MAIN')
                --and SEGMENTATION  In  ('Staff','B2B','B2C')
                AND e.TRANSACTION_DATE   between DATE_SUB('2020-01-29',1)and '2020-01-29'
        ) c on c.transaction_date = b.roaming_date
        join (-- MTD
            select '2020-01-29'sdate,SUM(TAXED_AMOUNT)  MTD_AMOUNT
            from  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY a
            LEFT JOIN DIM.DT_OFFER_PROFILES  ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
            where TRAFFIC_MEAN='REVENUE'
                and a.OPERATOR_CODE  In  ('OCM')
                and SUB_ACCOUNT  In  ('MAIN')
                --and SEGMENTATION  In  ('Staff','B2B','B2C')
                AND TRANSACTION_DATE between  CONCAT(SUBSTRING('2020-01-29',0,7),'-','01') and '2020-01-29'
        ) d on d.sdate = c.transaction_date
        join (-- LMTD
            select
             '2020-01-29' sdate
            ,SUM(TAXED_AMOUNT)*CAST(SUBSTRING('2020-01-29',9,2) AS INT)/CAST(SUBSTRING(add_months('2020-01-29',-1),9,2) AS INT) LMTD_AMOUNT
            from  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY a
            LEFT JOIN DIM.DT_OFFER_PROFILES   ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
            where TRAFFIC_MEAN='REVENUE'
                and a.OPERATOR_CODE  In  ('OCM')
                and SUB_ACCOUNT  In  ('MAIN')
               -- and SEGMENTATION  In  ('Staff','B2B','B2C')
                AND TRANSACTION_DATE between  add_months(CONCAT(SUBSTRING('2020-01-29',0,7),'-','01'),-1)
                and add_months('2020-01-29',-1)
        ) e on e.sdate = d.sdate
)B


CREATE TABLE JUNK.SOURCE_DATA_WITH_DATE AS
SELECT
    source_data,
    datecode
FROM
(select * from junk.source_data)a,
(select datecode from  DIM.DT_DATES where datecode between  '2019-12-01' and '2020-01-29')b


SELECT
    a.datecode,
    a.source_data
from JUNK.SOURCE_DATA_WITH_DATE a
left  join (
    select
        TRANSACTION_DATE,
        source_data
    from agg.spark_ft_global_activity_daily
    where transaction_date between '2019-12-01' and '2020-01-28'
)b on a.datecode=b.TRANSACTION_DATE and a.source_data=b.source_data
where b.source_data is null
order by 1,2

'2019-12-11','2019-12-17','2020-01-10','2020-01-17','2020-01-18','2020-01-19','2020-01-20','2020-01-21','2020-01-22','2020-01-23','2020-01-24','2020-01-25',
    EVENT_DATETIME|EVENT_TYPE|EVENT_QUANTITY|A_NUMBER|B_NUMBER|DIRECTION|LOCATION_ID|EQUIPMENT_ID|LOCATION_NAME_B|TRANSACTION_TYPE|SUBSCRIBER_TYPE|SOLDE|A_NAME|B_NAME|LOCATION_NAME_A|REGION|NVL(GEO_LOC, '') GEO_LOC