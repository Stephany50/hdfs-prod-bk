INSERT INTO MON.FT_OMNY_ACCOUNT_SNAPSHOT
            SELECT

            a.USER_ID,
            a.MSISDN ACCOUNT_NUMBER,
            a.MSISDN MSISDN,
            b.PARTNER_FIRST_NAME,
            b.PARTNER_LAST_NAME,
            NULL REGISTERED_ON,
            NULL CITY,
            NULL ADDRESS,
            NULL SEX,
            NULL ID_NUMBER,
            a.STATUS ACCOUNT_STATUS,
            NULL CREATED_BY,
            NULL FIRST_TRANSACTION_ON,
            NULL USER_DOMAIN,
            NULL USER_CATEGORY,
            NULL GEOGRAPHICAL_DOMAIN,
            a.USER_TYPE,
            NULL DELETED_ON,
            NULL DEACTIVATION_BY,
            NULL BILL_COMPANY_CODE,
            NULL COMPANY_TYPE,
            NULL NOTIFICATION_TYPE,
            NULL PROFILE_ID,
            NULL PARENT_USER_ID,
            NULL CREATION_DATE,
            NULL MODIFIED_BY,
            NULL MODIFIED_ON,
            NULL BIRTH_DATE,
            a.BALANCE ACCOUNT_BALANCE,
            NULL CREATED_BY_MSISDN,
            current_timestamp INSERT_DATE,
            "###SLICE_VALUE###" EVENT_DATE
            FROM (
            SELECT USER_ID, MSISDN, STATUS, USER_TYPE, sum(BALANCE) BALANCE
            FROM CDR.IT_OMNY_OPERATOR_BALANCE
            WHERE TO_DATE(ORIGINAL_FILE_DATE)="###SLICE_VALUE###"
            --AND USER_ID IN ('BK130405.1017.026385')
            GROUP BY
              USER_ID,
              MSISDN,
              STATUS,
              USER_TYPE
            ) a
            LEFT JOIN DIM.DT_OM_BANK_PARTNER_ACCOUNT b
            ON a.USER_ID=b.ACCOUNT_ID
            LEFT JOIN
            (
                SELECT * FROM MON.FT_OMNY_ACCOUNT_SNAPSHOT WHERE EVENT_DATE = "###SLICE_VALUE###"
            ) C ON A.USER_ID = C.USER_ID
            WHERE C.USER_ID IS NULL;