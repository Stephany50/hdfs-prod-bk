SELECT (IF S_RESULT = 0)

           THEN
                WITH APGL AS
                (
                   SELECT

                        A.*,

                        B1.SR_USER_TYPE SENDER_USER_TYPE,

                        B2.SR_USER_TYPE RECEIVER_USER_TYPE
                    FROM

                    (
                        SELECT *
                        FROM TANGO_CDR.IT_OMNY_APGL
                        WHERE TRANSACTION_DATE_TIME = '###SLICE_VALUE###'
                    ) A
                    LEFT JOIN DIM.DT_OM_PARTNER_SETUP B1 ON (UPPER(A.SENDER_CATEGORY_CODE) = UPPER(B1.SR_CATEGORY_CODE))
                    LEFT JOIN DIM.DT_OM_PARTNER_SETUP B2 ON (UPPER(A.RECEIVER_CATEGORY_CODE) = UPPER(B2.SR_CATEGORY_CODE))
                )
                SELECT COUNT(*) INTO S_RESULT
                FROM APGL A
                LEFT JOIN DIM.DT_OM_TRANS_SETUP B ON (
                       A.TRANSACTION_TAG = B.TRANSACTION_TYPE
                    AND A.SENDER_USER_TYPE = B.SENDER_USER_TYPE
                    AND A.RECEIVER_USER_TYPE = B.RECEIVER_USER_TYPE
                )
                WHERE ACCOUNT_NO IS NULL;
                IF (S_RESULT <> 0) THEN

                    RAISE_APPLICATION_ERROR  (-20001, S_RESULT || ' combinaison(s) (TRANSACTION_TAG, SENDER_USER_TYPE, RECEIVER_USER_TYPE) non referenciée(s) detectée(s) dans les écritures APGL du ' || '###SLICE_VALUE###' )