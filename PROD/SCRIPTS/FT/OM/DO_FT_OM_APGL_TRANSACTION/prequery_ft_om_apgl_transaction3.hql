IF S_RESULT = 1
       THEN
            SELECT COUNT(*) INTO S_RESULT
            FROM
            (
                SELECT DISTINCT UPPER(SENDER_CATEGORY_CODE) CODE
                FROM TANGO_CDR.IT_OMNY_APGL

                WHERE TRANSACTION_DATE_TIME = '###SLICE_VALUE###'
                MINUS
                SELECT DISTINCT UPPER(SR_CATEGORY_CODE) CODE
                FROM DIM.DT_OM_PARTNER_SETUP
                UNION ALL
                SELECT DISTINCT UPPER(RECEIVER_CATEGORY_CODE) CODE
                FROM TANGO_CDR.IT_OMNY_APGL
                WHERE TRANSACTION_DATE_TIME = '###SLICE_VALUE###'
                MINUS
                SELECT DISTINCT UPPER(SR_CATEGORY_CODE) CODE
                FROM DIM.DT_OM_PARTNER_SETUP
            )

