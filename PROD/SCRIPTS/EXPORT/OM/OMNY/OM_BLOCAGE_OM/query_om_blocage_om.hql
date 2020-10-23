SELECT
    ACTION,
    TO_DATE(SYS_TIMESTAMP) SYS_TIMESTAMP,
    MSISDN,
    PARTY_USER_ID,
    USER_NAME,
    PARENT_NAME,
    PARTY_LOGIN_ID,
    TYPE,
    BARRING_REASON,
    ACTION_PERFORMED_BY,
    '0' STATUS
FROM CDR.SPARK_IT_OMNY_BARRED_ACCOUNTS
WHERE ORIGINAL_FILE_DATE ='###SLICE_VALUE###'