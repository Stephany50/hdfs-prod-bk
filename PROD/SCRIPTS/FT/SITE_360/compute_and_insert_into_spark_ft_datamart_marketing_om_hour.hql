
insert into MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
select
    sender_msisdn
    , receiver_msisdn
    , a.hour_period event_hour
    , transaction_amount
    , revenu_om
    , service_type
    , style
    , merchant_code
    , merchant_fist_name
    , merchant_last_name
    , merchant_short_name
    , site_name
    , town
    , region
    , commercial_region
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        sender_msisdn,
        receiver_msisdn,
        hour_period,
        service_type,
        style,
        sum(transaction_amount) transaction_amount,
        sum(service_charge_received) revenu_om
    from
    (
        -- RECHARGE
        -- AIRTIME
        -- SELF
        SELECT 
            SENDER_MSISDN,
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TOP_UP' STYLE
        FROM cdr.spark_it_omny_transactions
        WHERE transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('RC') AND SENDER_CATEGORY_CODE='SUBS' AND OTHER_MSISDN IS NULL
            AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- TIERS_SELF
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TOP_UP' STYLE
        FROM cdr.spark_it_omny_transactions
        WHERE transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('RC') AND SENDER_CATEGORY_CODE='SUBS' AND OTHER_MSISDN IS NOT NULL
            AND OTHER_MSISDN=SENDER_MSISDN AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- TIERS
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TOP_UP' STYLE
        FROM cdr.spark_it_omny_transactions
        WHERE transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('RC') AND SENDER_CATEGORY_CODE='SUBS' AND OTHER_MSISDN IS NOT NULL
            AND OTHER_MSISDN<>SENDER_MSISDN AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- INTERNET
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'RECHARGE' STYLE
        FROM cdr.spark_it_omny_transactions
        WHERE transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS'
            AND RECEIVER_MSISDN IN (SELECT MSISDN FROM dim.om_INTERNET_MSISDNS) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        UNION ALL
        -- BILL PAYMENTS
        -- ENEO
        -- ENEO PREPAID
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            (
                (
                    CASE 
                        WHEN SERVICE_CHARGE_RECEIVED > 0 THEN SERVICE_CHARGE_RECEIVED
                        WHEN TRANSACTION_AMOUNT BETWEEN 0 AND 4999 THEN 100
                        WHEN TRANSACTION_AMOUNT BETWEEN 5000 AND 10000 THEN 220
                        WHEN TRANSACTION_AMOUNT BETWEEN 10001 AND 25000 THEN 400
                        WHEN TRANSACTION_AMOUNT BETWEEN 25001 AND 50000 THEN 400
                        WHEN TRANSACTION_AMOUNT BETWEEN 50001 AND 100000 THEN 500
                    ELSE 750 END
                )/1.1925
            ) * 0.6 service_charge_received,
            'FACTURIER' STYLE
        FROM cdr.spark_it_omny_transactions X JOIN dim.om_MSISDN_ENEO Y ON X.RECEIVER_MSISDN=Y.MSISDN
        WHERE transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('MERCHPAY', 'BILLPAY') AND SENDER_CATEGORY_CODE='SUBS' 
            and SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- CANAL
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'FACTURIER' STYLE
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN  IN (SELECT MSISDN FROM dim.om_MSISDN_CANAL) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        UNION ALL
        -- CDE
        -- CAMWATER
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'FACTURIER' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY', 'BILLPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN  IN (SELECT MSISDN FROM dim.om_MSISDN_EAU) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- ORANGE
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'FACTURIER' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('BILLPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN IN ('orang') AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- ALL BILL PAY ASSURANCE 
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'FACTURIER' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('BILLPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_ENEO UNION ALL SELECT MSISDN FROM dim.om_MSISDN_EAU UNION SELECT 'orang' MSISDN UNION ALL SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
            AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- ASSUR TOUS
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'MICRO-ASSURANCE' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN IN (SELECT MSISDN FROM dim.om_MSISDN_ASSUR_TOUS) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- ATLANTIC ASSURANCE
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            transaction_amount * 0.07 service_charge_received, 
            'MICRO-ASSURANCE' STYLE
        FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
        WHERE TRANSFER_STATUS='TS' AND TRANSFER_DATETIME = '###SLICE_VALUE###' AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN IN (SELECT MSISDN FROM dim.om_MSISDN_ATLANTIC_ASSURANCE) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- ACTIVA
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'MICRO-ASSURANCE' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN IN (SELECT MSISDN FROM dim.om_MSISDN_ACTIVA) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- TRANSFERS
        -- TNO_FREE_DEST
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TNO' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('P2PNONREG') AND 
            SENDER_CATEGORY_CODE='SUBS' AND TNO_MSISDN IS NULL
        -- TNO_DEST
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TNO' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('P2PNONREG') AND SENDER_CATEGORY_CODE='SUBS' AND 
            TNO_MSISDN IS NOT NULL AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS) AND 
            RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- MARCHAND FOCUS
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'MARCHAND' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_NON_MERCHANT_MSISDNS) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL 
            SELECT MSISDN FROM dim.om_NON_MERCHANT_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_B2W UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND RECEIVER_GRADE_NAME NOT IN ('gradeMinsc')
        -- MARCHAND MINSEC
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'MARCHAND' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_GRADE_NAME IN ('gradeMinsc') AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL 
            SELECT MSISDN FROM dim.om_NON_MERCHANT_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_B2W)
        -- DISTRIBUTION 
        -- CASHOUT 
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'DISTRIBUTION' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('CASHOUT') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN NOT IN  (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_VISA UNION ALL 
            SELECT MSISDN FROM dim.om_MSISDN_B2W  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_VISA UNION ALL 
            SELECT MSISDN FROM dim.om_MSISDN_B2W  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
        -- CASH IN 
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'DISTRIBUTION' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('CASHIN') AND RECEIVER_CATEGORY_CODE='SUBS' AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND 
            RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
        -- W2B
        -- ECOBANK,
        -- AFRILAND,
        -- UBA,
        -- PANAFRICA,
        -- ADVANS
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'B2W' style
        FROM cdr.spark_it_omny_transactions A JOIN dim.om_MSISDN_B2W B 
        ON A.RECEIVER_MSISDN=B.MSISDN
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('CASHOUT') AND SENDER_CATEGORY_CODE='SUBS' AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- B2W
        -- ECOBANK
        UNION ALL 
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'B2W' style
        FROM cdr.spark_it_omny_transactions A JOIN dim.om_MSISDN_B2W B 
        ON A.SENDER_MSISDN=B.MSISDN
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('CASHIN') AND RECEIVER_CATEGORY_CODE='SUBS' 
            AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- VISA 
        -- PAYMENT
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'VISA' style 
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('MERCHPAY') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN  IN (SELECT MSISDN FROM dim.om_MSISDN_VISA) AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- VISA CASHOUT
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'VISA' style
        FROM cdr.spark_it_omny_transactions
        WHERE transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('CASHOUT') AND SENDER_CATEGORY_CODE='SUBS' AND 
            RECEIVER_MSISDN  IN (SELECT MSISDN FROM dim.om_MSISDN_VISA)
        -- P2P
        -- P2P TO NOT A SUB
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TRANSFERT' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('P2P') AND SENDER_CATEGORY_CODE = 'SUBS' AND 
            RECEIVER_CATEGORY_CODE NOT IN ('SUBS') AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
            AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
        -- P2P TO A SUB
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TRANSFERT' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('P2P') AND SENDER_CATEGORY_CODE = 'SUBS' 
            AND RECEIVER_CATEGORY_CODE IN ('SUBS') AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
            AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
        -- UNKNOWN RECEIVER
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TRANSFERT' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('P2P','OTF') AND RECEIVER_CATEGORY_CODE='SUBS' AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND SENDER_CATEGORY_CODE NOT IN ('SUBS')
            AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS  UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC)
        -- P2P UNKNOWN SENDER
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'TRANSFERT' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('P2P','OTF') AND RECEIVER_CATEGORY_CODE='SUBS' AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS UNION ALL SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND SENDER_CATEGORY_CODE IN ('SUBS') 
            AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- SALARY PAYMENTS 
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'SALAIRE' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SERVICE_TYPE IN ('ENT2REG') AND RECEIVER_CATEGORY_CODE='SUBS' AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS) AND 
            RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS)
        -- GIMAC
        -- IN
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'GIMAC' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND SENDER_MSISDN IN (SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND 
            RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS) AND
            RECEIVER_CATEGORY_CODE='SUBS'
        -- GIMAC
        -- OUT
        UNION ALL
        SELECT 
            SENDER_MSISDN, 
            receiver_msisdn,
            from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
            service_type,
            transaction_amount,
            service_charge_received,
            'GIMAC' style
        FROM cdr.spark_it_omny_transactions
        where transfer_datetime = '###SLICE_VALUE###' and  TRANSFER_STATUS='TS'
            AND RECEIVER_MSISDN IN (SELECT MSISDN FROM dim.om_MSISDN_GIMAC) AND 
            SENDER_MSISDN NOT IN (SELECT MSISDN FROM dim.om_MSISDN_B2W UNION SELECT MSISDN FROM dim.om_OM_TEST_MSISDNS) AND 
            SENDER_CATEGORY_CODE='SUBS'
    ) a0
    group by sender_msisdn,
        receiver_msisdn,
        hour_period,
        service_type,
        style
) a
left join
(
    select
        msisdn,
        hour_period,
        site_name,
        town,
        region,
        commercial_region
    from mon.spark_ft_client_site_traffic_hour
    where event_date = '###SLICE_VALUE###'
) b on a.sender_msisdn = b.msisdn and a.hour_period = b.hour_period
left join
(
    select
        msisdn,
        user_grade_code,
        agent_code merchant_code,
        nvl(user_first_name, '') merchant_fist_name,
        nvl(user_last_name, '') merchant_last_name,
        nvl(user_short_name, '') merchant_short_name
    from cdr.spark_it_om_all_users
    where original_file_date = '###SLICE_VALUE###' and trim(upper(user_grade_code)) in (select trim(upper(user_grade_code)) from dim.dt_om_merchant_user_grade_codes)
) c on a.receiver_msisdn = c.msisdn