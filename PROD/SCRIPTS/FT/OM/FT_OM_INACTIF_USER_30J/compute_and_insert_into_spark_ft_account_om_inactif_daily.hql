INSERT INTO MON.SPARK_FT_ACCOUNT_OM_INACTIF_DAILY
SELECT 
    account_number msisdn,
    '###SLICE_VALUE###' event_date
FROM 
    (SELECT 
        DISTINCT account_number
    FROM
        (SELECT 
        account_number
        FROM 
        (SELECT  
            trim(account_number) account_number
        FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE EVENT_DATE=date_sub('###SLICE_VALUE###',1))
        minus
        (SELECT 
            trim(sender_msisdn) account_number
        FROM cdr.spark_it_omny_transactions 
        WHERE transfer_datetime = '###SLICE_VALUE###' AND trim(UPPER(sender_msisdn)) <> 'NA' AND TRANSFER_STATUS='TS' 
        AND SENDER_CATEGORY_CODE='SUBS' AND SERVICE_TYPE IN ('MERCHPAY','P2PNONREG','CASHOUT','BILLPAY','P2P','RC'))
        minus
        (SELECT 
            trim(receiver_msisdn) account_number
        FROM cdr.spark_it_omny_transactions 
        WHERE transfer_datetime = '###SLICE_VALUE###' AND trim(UPPER(receiver_msisdn)) <> 'NA'AND TRANSFER_STATUS='TS' 
        AND RECEIVER_CATEGORY_CODE='SUBS' AND SERVICE_TYPE IN ('CASHIN','P2P','ENT2REG')) ) RES )RES_F

