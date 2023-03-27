INSERT INTO CDR.SPARK_IT_OMNY_TRANSACTIONS PARTITION (TRANSFER_DATETIME,FILE_DATE)
SELECT
    SENDER_MSISDN,
    RECEIVER_MSISDN,
    RECEIVER_USER_ID,
    SENDER_USER_ID,
    TRANSACTION_AMOUNT,
    COMMISSIONS_PAID,
    COMMISSIONS_RECEIVED,
    COMMISSIONS_OTHERS,
    SERVICE_CHARGE_RECEIVED,
    SERVICE_CHARGE_PAID,
    TAXES,
    SERVICE_TYPE,
    TRANSFER_STATUS,
    SENDER_PRE_BAL,
    SENDER_POST_BAL,
    RECEIVER_PRE_BAL,
    RECEIVER_POST_BAL,
    SENDER_ACC_STATUS,
    RECEIVER_ACC_STATUS,
    ERROR_CODE,
    ERROR_DESC,
    REFERENCE_NUMBER,
    FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_ON,'dd/MM/yyyy HH:mm:ss')) CREATED_ON,
    CREATED_BY,
    FROM_UNIXTIME(UNIX_TIMESTAMP(MODIFIED_ON,'dd/MM/yyyy HH:mm:ss')) MODIFIED_ON,
    MODIFIED_BY,
    FROM_UNIXTIME(UNIX_TIMESTAMP(APP_1_DATE,'dd/MM/yyyy HH:mm:ss')) APP_1_DATE,
    FROM_UNIXTIME(UNIX_TIMESTAMP(APP_2_DATE,'dd/MM/yyyy HH:mm:ss')) APP_2_DATE,
    TRANSFER_ID,
    FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME,'dd/MM/yyyy HH:mm:ss')) TRANSFER_DATETIME_NQ,
    SENDER_CATEGORY_CODE,
    SENDER_DOMAIN_CODE,
    SENDER_GRADE_NAME,
    SENDER_GROUP_ROLE,
    SENDER_DESIGNATION,
    SENDER_STATE,
    RECEIVER_CATEGORY_CODE,
    RECEIVER_DOMAIN_CODE,
    RECEIVER_GRADE_NAME,
    RECEIVER_GROUP_ROLE,
    RECEIVER_DESIGNATION,
    RECEIVER_STATE,
    SENDER_CITY,
    RECEIVER_CITY,
    APP_1_BY,
    APP_2_BY,
    REQUEST_SOURCE,
    GATEWAY_TYPE,
    TRANSFER_SUBTYPE,
    PAYMENT_TYPE,
    PAYMENT_NUMBER,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(PAYMENT_DATE,'dd/MM/yyyy'))) PAYMENT_DATE,
    REMARKS,
    ACTION_TYPE,
    TRANSACTION_TAG,
    RECONCILIATION_BY,
    RECONCILIATION_FOR,
    EXT_TXN_NUMBER,
    ORIGINAL_REF_NUMBER,
    ZEBRA_AMBIGUOUS,
    ATTEMPT_STATUS,
    OTHER_MSISDN,
    SENDER_WALLET_NUMBER,
    RECEIVER_WALLET_NUMBER,
    SENDER_USER_NAME,
    RECEIVER_USER_NAME,
    TNO_MSISDN,
    TNO_ID,
    UNREG_FIRST_NAME,
    UNREG_LAST_NAME,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(UNREG_DOB,'dd/MM/yyyy'))) UNREG_DOB,
    UNREG_ID_NUMBER,
    BULK_PAYOUT_BATCHID,
    IS_FINANCIAL,
    TRANSFER_DONE,
    INITIATOR_MSISDN,
    VALIDATOR_MSISDN,
    INITIATOR_COMMENTS,
    VALIDATOR_COMMENTS,
    SENDER_WALLET_NAME,
    RECIEVER_WALLET_NAME,
    SENDER_USER_TYPE,
    RECEIVER_USER_TYPE,
    ORIGINAL_FILE_NAME ,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TXNMODE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME,'dd/MM/yyyy HH:mm:ss'))) TRANSFER_DATETIME,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) FILE_DATE
FROM CDR.TT_OMNY_TRANSACTIONS C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM  CDR.SPARK_IT_OMNY_TRANSACTIONS) T ON T.file_name = C.original_file_name WHERE  T.file_name IS NULL
