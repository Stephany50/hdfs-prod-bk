SELECT IF(T_1.S_RESULT = 0 and T_2.S_RESULT= 0  ,"OK","NOK") S_RESULT_EXIST
FROM
(SELECT COUNT(*) S_RESULT FROM (
        SELECT DISTINCT UPPER(SENDER_CATEGORY_CODE) CODE
                FROM CDR.IT_OM_APGL
                WHERE original_file_date = '###SLICE_VALUE###'
                MINUS
                SELECT DISTINCT UPPER(SR_CATEGORY_CODE) CODE
                FROM DIM.DT_OM_PARTNER_SETUP
                UNION ALL
                SELECT DISTINCT UPPER(RECEIVER_CATEGORY_CODE) CODE
                FROM CDR.IT_OM_APGL
                WHERE original_file_date = '###SLICE_VALUE###'
                MINUS
                SELECT DISTINCT UPPER(SR_CATEGORY_CODE) CODE
                FROM DIM.DT_OM_PARTNER_SETUP
    ) T
) T_1,
(SELECT COUNT(*)  S_RESULT
 FROM (
    SELECT  A.*,
        B1.SR_USER_TYPE SENDER_USER_TYPE,
        B2.SR_USER_TYPE RECEIVER_USER_TYPE
    FROM (
          SELECT *
          FROM CDR.IT_OM_APGL
          WHERE original_file_date = '###SLICE_VALUE###'
         ) A
    LEFT JOIN DIM.DT_OM_PARTNER_SETUP B1 ON (UPPER(A.SENDER_CATEGORY_CODE) = UPPER(B1.SR_CATEGORY_CODE))
    LEFT JOIN DIM.DT_OM_PARTNER_SETUP B2 ON (UPPER(A.RECEIVER_CATEGORY_CODE) = UPPER(B2.SR_CATEGORY_CODE))
) C
LEFT JOIN DIM.DT_OM_TRANS_SETUP B ON (
    C.TRANSACTION_TAG = B.TRANSACTION_TYPE
    AND C.SENDER_USER_TYPE = B.SENDER_USER_TYPE
    AND C.RECEIVER_USER_TYPE = B.RECEIVER_USER_TYPE
)
WHERE ACCOUNT_NO IS NULL
) T_2