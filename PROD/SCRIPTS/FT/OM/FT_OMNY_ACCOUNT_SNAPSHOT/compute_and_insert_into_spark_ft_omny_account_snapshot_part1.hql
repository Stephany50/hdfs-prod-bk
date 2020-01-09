INSERT INTO MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
            SELECT
            a.USER_ID,
            a.MSISDN ACCOUNT_NUMBER,
            a.MSISDN MSISDN,
            a.USER_FIRST_NAME,
            a.USER_LAST_NAME,
            b.REGISTERED_ON,
            NULL CITY,
            NULL ADDRESS,
            NULL SEX,
            b.ID_NUMBER,
            b.ACCOUNT_STATUS,
            NULL CREATED_BY,
            NULL FIRST_TRANSACTION_ON,
            a.USER_DOMAIN,
            a.USER_CATEGORY,
            NULL GEOGRAPHICAL_DOMAIN,
            a.USER_TYPE,
            NULL DELETED_ON,
            NULL DEACTIVATION_BY,
            NULL BILL_COMPANY_CODE,
            NULL COMPANY_TYPE,
            NULL NOTIFICATION_TYPE,
            NULL PROFILE_ID,
            NULL PARENT_USER_ID,
            b.CREATION_DATE,
            NULL MODIFIED_BY,
            b.MODIFIED_ON,
            b.BIRTH_DATE,
            a.ACCOUNT_BALANCE,
            NULL CREATED_BY_MSISDN,
            current_timestamp INSERT_DATE,
            "###SLICE_VALUE###" EVENT_DATE
            FROM
            (
                SELECT
                ACCOUNT_NAME USER_ID,
                ACCOUNT_ID MSISDN,
                USER_NAME USER_FIRST_NAME,
                LAST_NAME USER_LAST_NAME,
                USER_DOMAIN,
                USER_CATEGORY,
                ACCOUNT_TYPE USER_TYPE,
                BALANCE ACCOUNT_BALANCE
                FROM CDR.SPARK_IT_OM_ALL_BALANCE
                WHERE TO_DATE(ORIGINAL_FILE_DATE)="###SLICE_VALUE###"
            ) a
            LEFT JOIN
            (
                SELECT DISTINCT
                USER_ID,
                REGISTERED_ON,
                ID_NUMBER,
                ACCOUNT_STATUS,
                CREATION_DATE,
                MODIFIED_ON,
                BIRTH_DATE
                FROM
                (
                SELECT
                USER_ID,
                CREATED_ON REGISTERED_ON,
                EXTERNAL_CODE ID_NUMBER,
                IS_ACTIVE ACCOUNT_STATUS,
                CREATED_ON CREATION_DATE,
                MODIFIED_ON MODIFIED_ON,
                BIRTH_DATE BIRTH_DATE,
                ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY MODIFIED_ON DESC)  AS RANG
                FROM CDR.SPARK_IT_OMNY_ACCOUNT_SNAPSHOT
                WHERE TO_DATE(ORIGINAL_FILE_DATE)= DATE_ADD("###SLICE_VALUE###",1)
                )m
                WHERE RANG=1
            ) b
            ON a.USER_ID=b.USER_ID


