INSERT INTO MON.SPARK_FT_OTHER_VAS
SELECT
    COMMAND_NAME RECORD_ID,
    DATE_FORMAT(START_DATE_TIME, 'HH:mm:ss') TRANSACTION_TIME,
    MSISDN_SRC SERVED_PARTY_MSISDN,
    NULL OTHER_PARTY_MSISDN,
    MSISDN_SRC BENEFICIARY_PARTY,
    DURATION/1000 RATED_VOLUME,
    'seconde' UNIT_OF_MEASUREMENT,
    TOTAL_AMOUNT_DEBIT RATED_AMOUNT,
    NULL RATED_AMOUNT_IN_BUNDLE,
    NULL BUNDLE_ID,
    'FCFA' BUNDLE_UNIT,
    'VEXT' SERVICE_CODE,
    'P2P' APPLICATION_NAME,
    IF(RETURN_CODE = '000','1','0') TRANSACTION_TERMINATION_IND,
    NULL TRANSACTION_DESTINATION_CODE,
    b.MAIN_IMSI SERVED_PARTY_IMSI,
    NULL SERVED_PARTY_IMEI,
    b.OSP_ACCOUNT_TYPE SUBSCRIBER_TYPE,
    NULL EVENT_TYPE,
    a.ORIGINAL_FILE_NAME ORIGINAL_FILE_NAME,
    CURRENT_DATE DWH_ENTRY_DATE ,
    b.PROFILE COMMERCIAL_OFFER_CODE,
    NULL INFO2,
    START_DATE TRANSACTION_DATE
FROM CDR.SPARK_IT_P2P_LOG a
         LEFT JOIN mon.ft_contract_snapshot b ON (FN_GET_NNP_MSISDN_9DIGITS(a.MSISDN_SRC)=b.ACCESS_KEY)
where
        START_DATE = '###SLICE_VALUE###'
  AND SUBSTR(TRIM(ussd_order),1,3) = 'TPS'
  and b.EVENT_DATE = '###SLICE_VALUE###'
  AND b.OSP_STATUS <> 'TERMINATED'
