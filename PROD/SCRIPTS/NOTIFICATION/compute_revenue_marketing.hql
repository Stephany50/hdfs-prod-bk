INSERT INTO MON.REVENUE_MARKETING
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    TRANSACTION_DATE
FROM(
    SELECT *
    FROM  dim.dt_smsnotification_recipient
    WHERE type='SMSREVENUMKT' AND actif='YES'
)A
LEFT JOIN (
    select
        a.TRANSACTION_DATE,
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
                    '2019-08-28' TRANSACTION_DATE,
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
                FROM  AGG.FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
                WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE = '2019-08-28'
            )T1
            LEFT JOIN (
                SELECT '2019-08-28' TRANSACTION_DATE,
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
                FROM  AGG.FT_GLOBAL_ACTIVITY_DAILY f, dim.dt_usages
                WHERE TRAFFIC_MEAN='REVENUE'  and f.OPERATOR_CODE = 'OCM' and SUB_ACCOUNT = 'MAIN' AND f.TRANSACTION_DATE   = DATE_SUB('2019-08-28',1)
            )T2 ON T1.TRANSACTION_DATE=T2.TRANSACTION_DATE

        ) a
        join (

            select max(transaction_date) transaction_date,
                sum(case when transaction_date = '2019-08-28' then main_rated_amount
                    else 0
                end) ca_roaming_out,
                sum(case when transaction_date = DATE_SUB('2019-08-28',1) then main_rated_amount
                    else 0
                end) ca_roaming_out_yd
            from AGG.FT_GSM_TRAFFIC_REVENUE_DAILY
            where transaction_date   between DATE_SUB('2019-08-28',1) and '2019-08-28' and destination like '%ROAM%'
        ) b on b.transaction_date = a.transaction_date
        join (
            select
            max(TRANSACTION_DATE) transaction_date,
            SUM(case when transaction_date = '2019-08-28' then TAXED_AMOUNT
                    else 0
                end) TOTAL_AMOUNT,
            SUM(case when transaction_date = DATE_SUB('2019-08-28',1)  then TAXED_AMOUNT
                    else 0
                end) TOTAL_AMOUNT_yd
            FROM  AGG.FT_GLOBAL_ACTIVITY_DAILY e
            LEFT JOIN DIM.DT_OFFER_PROFILES ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
            WHERE TRAFFIC_MEAN='REVENUE'
                and e.OPERATOR_CODE  In  ('OCM')
                and SUB_ACCOUNT  In  ('MAIN')
                and SEGMENTATION  In  ('Staff','B2B','B2C')
                AND e.TRANSACTION_DATE   between DATE_SUB('2019-08-28',1)and '2019-08-28'
        ) c on c.transaction_date = b.transaction_date
        join (-- MTD
            select '2019-08-28'sdate,SUM(TAXED_AMOUNT)  MTD_AMOUNT
            from  AGG.FT_GLOBAL_ACTIVITY_DAILY a
            LEFT JOIN DIM.DT_OFFER_PROFILES  ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
            where TRAFFIC_MEAN='REVENUE'
                and a.OPERATOR_CODE  In  ('OCM')
                and SUB_ACCOUNT  In  ('MAIN')
                and SEGMENTATION  In  ('Staff','B2B','B2C')
                AND TRANSACTION_DATE between  CONCAT(SUBSTRING('2019-08-28',0,7),'-','01') and '2019-08-28'
        ) d on d.sdate = c.transaction_date
        join (-- LMTD
            select
             '2019-08-28' sdate
            ,SUM(TAXED_AMOUNT)*CAST(SUBSTRING('2019-08-28',9,2) AS INT)/CAST(SUBSTRING(add_months('2019-08-28',-1),9,2) AS INT) LMTD_AMOUNT
            from  AGG.FT_GLOBAL_ACTIVITY_DAILY a
            LEFT JOIN DIM.DT_OFFER_PROFILES   ON PROFILE_CODE = upper(COMMERCIAL_OFFER_CODE)
            where TRAFFIC_MEAN='REVENUE'
                and a.OPERATOR_CODE  In  ('OCM')
                and SUB_ACCOUNT  In  ('MAIN')
                and SEGMENTATION  In  ('Staff','B2B','B2C')
                AND TRANSACTION_DATE between  add_months(CONCAT(SUBSTRING('2019-08-28',0,7),'-','01'),-1)
                and add_months('2019-08-28',-1)
        ) e on e.sdate = d.sdate
)B