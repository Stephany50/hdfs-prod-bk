INSERT INTO MON.SPARK_FT_OM_ACTIVE_USER
SELECT
(CASE WHEN SERVICE_TYPE IN ('P2P') THEN 'P2P_OUT'
       WHEN SERVICE_TYPE IN ('P2PNONREG') THEN 'P2P_TNO'
       WHEN SERVICE_TYPE='BILLPAY' AND RECEIVER_MSISDN='orang' THEN 'ORANGE_BILLPAY'
       WHEN SERVICE_TYPE='BILLPAY' AND RECEIVER_MSISDN='AES' THEN 'AES_BILLPAY'
       WHEN SERVICE_TYPE='BILLPAY' AND RECEIVER_MSISDN='ACTI' THEN  'ACTI_BILLPAY'
       WHEN SERVICE_TYPE='MERCHPAY' AND fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='699703939' THEN  'CANALPLUS_BILLPAY'
       WHEN SERVICE_TYPE='MERCHPAY' AND fn_format_msisdn_to_9digits(RECEIVER_MSISDN)<>'699703939' THEN  'OTHER_MERCHPAY'
 ELSE SERVICE_TYPE  END ) SERVICE_TYPE
,fn_format_msisdn_to_9digits(SENDER_MSISDN) MSISDN
,SUM(TRANSACTION_AMOUNT) TRANSACTION_AMOUNT
,MAX(TRANSFER_DATETIME) LAST_TRANSACTION_DATE_TIME
,CURRENT_TIMESTAMP() INSERT_DATE
,TRANSFER_DATETIME EVENT_DATE
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME BETWEEN DATE_SUB('###SLICE_VALUE###',29)  AND '###SLICE_VALUE###' --ADD_MONTHS(v_process_date,-1) AND v_process_date
AND TRANSFER_STATUS='TS'
AND SENDER_CATEGORY_CODE='SUBS'
AND SERVICE_TYPE IN ('MERCHPAY','P2PNONREG','CASHOUT','BILLPAY','P2P','RC')
GROUP BY TRANSFER_DATETIME,fn_format_msisdn_to_9digits(SENDER_MSISDN)
,(CASE WHEN SERVICE_TYPE IN ('P2P') THEN 'P2P_OUT'
       WHEN SERVICE_TYPE IN ('P2PNONREG') THEN 'P2P_TNO'
       WHEN SERVICE_TYPE='BILLPAY' AND RECEIVER_MSISDN='orang' THEN 'ORANGE_BILLPAY'
       WHEN SERVICE_TYPE='BILLPAY' AND RECEIVER_MSISDN='AES' THEN 'AES_BILLPAY'
       WHEN SERVICE_TYPE='BILLPAY' AND RECEIVER_MSISDN='ACTI' THEN  'ACTI_BILLPAY'
       WHEN SERVICE_TYPE='MERCHPAY' AND fn_format_msisdn_to_9digits(RECEIVER_MSISDN)='699703939' THEN  'CANALPLUS_BILLPAY'
       WHEN SERVICE_TYPE='MERCHPAY' AND fn_format_msisdn_to_9digits(RECEIVER_MSISDN)<>'699703939' THEN  'OTHER_MERCHPAY'
 ELSE SERVICE_TYPE  END )