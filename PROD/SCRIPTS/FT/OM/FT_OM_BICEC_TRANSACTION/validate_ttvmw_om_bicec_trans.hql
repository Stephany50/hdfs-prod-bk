SELECT IF (ABS(AMOUNT - T.N_TOTAL) <100, 'OK', 'NOK')
FROM
(
    SELECT ROUND(SUM(AMOUNT)*2) AMOUNT
    from
    (
        SELECT SUM(NVL (TRANSACTION_AMOUNT, 0)) AMOUNT
        FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
        WHERE (TRANSFER_STATUS='TS' OR (TRANSFER_STATUS='TF'AND  RECONCILIATION_BY IS NOT NULL) OR (TRANSFER_STATUS='TF'AND SENDER_PRE_BAL<>SENDER_POST_BAL))
            AND TO_DATE(TRANSFER_DATETIME) >='###SLICE_VALUE###'
            AND TO_DATE(TRANSFER_DATETIME) < DATE_ADD('###SLICE_VALUE###',1)
        UNION
        SELECT SUM(NVL (COMMISSION_AMOUNT, 0)*3) AMOUNT
        FROM CDR.SPARK_IT_OMNY_COMMISSION_DETAILS
        WHERE COMMISSION_AMOUNT>0
            AND TO_DATE(TRANSACTION_DATE) >='###SLICE_VALUE###'
            AND TO_DATE(TRANSACTION_DATE) < DATE_ADD('###SLICE_VALUE###',1)
        UNION
        SELECT SUM(NVL (SERVICE_CHARGE_AMOUNT, 0)*4) AMOUNT
        FROM CDR.SPARK_IT_OMNY_SERVICES_CHARGES_DETAILS
        WHERE SERVICE_CHARGE_AMOUNT>0
            AND TO_DATE(TRANSACTION_DATE) >='###SLICE_VALUE###'
            AND TO_DATE(TRANSACTION_DATE) < DATE_ADD('###SLICE_VALUE###',1)
    ) M
) R ,
(
    SELECT
        ABS(ROUND(SUM(
        (CASE WHEN LIB LIKE '%cmms%' THEN NVL(MON, 0) ELSE 0 END)*3 -- CMMS
        + (CASE WHEN LIB LIKE '%chrg%' THEN NVL(MON, 0) ELSE 0 END)*4  -- CHRG
        + (CASE WHEN LIB LIKE '%amnt%' THEN NVL(MON, 0) ELSE 0 END) --  MON
        ))) N_TOTAL
    FROM
    (
        SELECT
            AGE
            ,DEV
            ,CHA
            ,NCP
            ,SUF
            ,OPE
            ,UTI
            ,CLC
            ,DCO
            ,DVA
            ,MON
            ,SEN
            ,LIB
            ,PIE
            ,MAR
            ,AGSA
            ,AGEM
            ,AGDE
            ,DEVC
            ,MCTV
            ,PIEO
            ,NULL USER_ID
            ,NULL REMARK
        FROM
        (
            SELECT
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGE,
                (CASE
                    WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE
                    ELSE '001'
                END) DEV,
                (CASE
                    WHEN FINANCIAL_CHAPTER IS NOT NULL THEN FINANCIAL_CHAPTER
                    ELSE '384999'
                END) CHA,
                (CASE
                    WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
                    WHEN ACCOUNT_NUMBER IS NULL
                    AND    USER_CATEGORY_CODE='SUBS' THEN '38499999997'
                    ELSE '38499999999'
                END) NCP,
                '  ' SUF,
                (CASE
                    WHEN OPERATIONTYPE_CODE IS NOT NULL THEN OPERATIONTYPE_CODE
                    ELSE CONCAT(
                        'ERROR: SERVICE_TYPE '
                        ,A.SERVICE_TYPE
                        ,' ABSENT DE LA TABLE DIM.DT_OM_OPERATION_TYPE. EVENT DATE: '
                        , TO_DATE (EVENT_DATE)
                        , ',  ACCOUNT_NUMBER='
                        , ACCOUNT_NUMBER
                    )
                END) OPE,
                'AUTO' UTI,
                (CASE
                    WHEN ACCOUNT_KEY IS NOT NULL THEN ACCOUNT_KEY
                    WHEN ACCOUNT_KEY IS NULL
                    AND USER_CATEGORY_CODE='SUBS' THEN '55'
                    ELSE '49'
                END) CLC,
                TO_DATE (EVENT_DATE) DCO,
                TO_DATE (EVENT_DATE) DVA,
                AMOUNT MON,
                SENS SEN,
                SUBSTR(CONCAT(OM_OPERATION_NAME, '.', ' ', '.amnt'), 1, 30) LIB ,
                CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIE,
                ' ' MAR,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGSA,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGEM,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGDE,
                (CASE
                    WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE
                    ELSE '001'
                END)   DEVC,
                AMOUNT MCTV,
                CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO,
                EVENT_DATE,
                CURRENT_TIMESTAMP INSERT_DATE
            FROM
            (
                SELECT
                    N_ROWNUM,
                    EVENT_DATE,
                    A.SERVICE_TYPE,
                    USER_ID,
                    USER_CATEGORY_CODE,
                    AMOUNT,
                    SENS,
                    OM_OPERATION_NAME,
                    OM_OPERATION_DESC,
                    OPERATIONTYPE_CODE,
                    OM_OPERATION_CODE
                FROM
                (
                    SELECT
                        ROW_NUMBER() OVER(ORDER BY SENDER_USER_ID) N_ROWNUM,
                        TO_DATE(TRANSFER_DATETIME) EVENT_DATE,
                        SERVICE_TYPE,
                        SENDER_USER_ID USER_ID,
                        SENDER_CATEGORY_CODE USER_CATEGORY_CODE,
                        TRANSACTION_AMOUNT AMOUNT,
                        'D' SENS
                    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
                    WHERE (
                        TRANSFER_STATUS='TS'
                        OR (TRANSFER_STATUS='TF' AND RECONCILIATION_BY IS NOT NULL)
                        OR (TRANSFER_STATUS='TF' AND SENDER_PRE_BAL<>SENDER_POST_BAL)
                    ) AND TO_DATE(TRANSFER_DATETIME) >='###SLICE_VALUE###' AND TO_DATE(TRANSFER_DATETIME) < DATE_ADD('###SLICE_VALUE###',1)
                    UNION
                    SELECT
                        ROW_NUMBER() OVER(ORDER BY RECEIVER_USER_ID ) N_ROWNUM,
                        TO_DATE(TRANSFER_DATETIME) EVENT_DATE,
                        SERVICE_TYPE,
                        RECEIVER_USER_ID USER_ID,
                        RECEIVER_CATEGORY_CODE USER_CATEGORY_CODE,
                        TRANSACTION_AMOUNT AMOUNT,
                        'C' SENS
                    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
                    WHERE(
                        TRANSFER_STATUS='TS'
                        OR (TRANSFER_STATUS='TF' AND RECONCILIATION_BY IS NOT NULL)
                        OR (TRANSFER_STATUS='TF' AND SENDER_PRE_BAL<>SENDER_POST_BAL)
                    ) AND TO_DATE(TRANSFER_DATETIME) >='###SLICE_VALUE###'
                    AND TO_DATE(TRANSFER_DATETIME) < DATE_ADD('###SLICE_VALUE###',1)
                ) A
                LEFT JOIN DIM.DT_OM_OPERATION_TYPE C
                ON (A.SERVICE_TYPE = C.SERVICE_TYPE)
            ) A
            LEFT JOIN DIM.DT_OM_BANK_PARTNER_ACCOUNT B
            ON (A.USER_ID = B.ACCOUNT_ID)
            UNION ALL
            SELECT
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGE ,
                (CASE
                    WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE
                    ELSE '001'
                END) DEV ,
                (CASE
                    WHEN FINANCIAL_CHAPTER IS NOT NULL THEN FINANCIAL_CHAPTER
                    ELSE '384999'
                END) CHA ,
                (CASE
                    WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
                    WHEN ACCOUNT_NUMBER IS NULL
                    AND    USER_CATEGORY_CODE='SUBS' THEN '38499999997'
                    ELSE '38499999999'
                END) NCP ,
                '  ' SUF ,
                (CASE
                    WHEN OPERATIONTYPE_CODE IS NOT NULL THEN OPERATIONTYPE_CODE
                    ELSE CONCAT( 'ERROR: SERVICE_TYPE '
                        ,A.SERVICE_TYPE
                        ,' ABSENT DE LA TABLE DIM.DT_OM_OPERATION_TYPE. EVENT DATE: '
                        , TO_DATE (EVENT_DATE)
                        , ',  ACCOUNT_NUMBER='
                        , ACCOUNT_NUMBER
                    )
                END)   OPE ,
                'AUTO' UTI ,
                (CASE
                    WHEN ACCOUNT_KEY IS NOT NULL THEN ACCOUNT_KEY
                    WHEN ACCOUNT_KEY IS NULL
                    AND    USER_CATEGORY_CODE='SUBS' THEN '55'
                    ELSE '49'
                END) CLC ,
                TO_DATE(EVENT_DATE) DCO ,
                TO_DATE(EVENT_DATE) DVA ,
                AMOUNT MON ,
                SENS SEN ,
                SUBSTR (CONCAT(OM_OPERATION_NAME, '.', ' ', '.cmms'), 1, 30) LIB ,
                CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIE ,
                ' ' MAR ,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGSA ,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGEM ,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGDE ,
                (CASE
                    WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE
                    ELSE '001'
                END) DEVC ,
                AMOUNT MCTV ,
                CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO ,
                EVENT_DATE ,
                CURRENT_TIMESTAMP INSERT_DATE
            FROM
            (
                SELECT
                    ROW_NUMBER() OVER(ORDER BY PAYER_USER_ID) N_ROWNUM ,
                    TO_DATE(TRANSACTION_DATE) EVENT_DATE ,
                    SERVICE_TYPE ,
                    PAYER_USER_ID USER_ID ,
                    PAYER_CATEGORY_CODE USER_CATEGORY_CODE ,
                    COMMISSION_AMOUNT AMOUNT ,
                    'D' SENS
                FROM CDR.SPARK_IT_OMNY_COMMISSION_DETAILS
                WHERE COMMISSION_AMOUNT>0
                    AND TO_DATE(TRANSACTION_DATE) >='###SLICE_VALUE###'
                    AND TO_DATE(TRANSACTION_DATE) < DATE_ADD('###SLICE_VALUE###',1)
                UNION
                SELECT
                    ROW_NUMBER() OVER(ORDER BY PAYEE_USER_ID) N_ROWNUM ,
                    TO_DATE(TRANSACTION_DATE) EVENT_DATE ,
                    SERVICE_TYPE ,
                    PAYEE_USER_ID USER_ID ,
                    PAYEE_CATEGORY_CODE USER_CATEGORY_CODE ,
                    COMMISSION_AMOUNT AMOUNT ,
                    'C' SENS
                FROM CDR.SPARK_IT_OMNY_COMMISSION_DETAILS
                WHERE COMMISSION_AMOUNT>0
                    AND TO_DATE(TRANSACTION_DATE) >='###SLICE_VALUE###'
                    AND TO_DATE(TRANSACTION_DATE) < DATE_ADD('###SLICE_VALUE###',1)
            ) A
            LEFT JOIN DIM.DT_OM_BANK_PARTNER_ACCOUNT B
            ON (A.USER_ID=B.ACCOUNT_ID)
            LEFT JOIN DIM.DT_OM_OPERATION_TYPE C
            ON (A.SERVICE_TYPE = C.SERVICE_TYPE)
            UNION ALL
            SELECT
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGE ,
                (CASE
                    WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE
                    ELSE '001'
                END) DEV ,
                (CASE
                    WHEN FINANCIAL_CHAPTER IS NOT NULL THEN FINANCIAL_CHAPTER
                    ELSE '384999'
                END) CHA ,
                (CASE
                    WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
                    WHEN ACCOUNT_NUMBER IS NULL
                    AND    USER_CATEGORY_CODE='SUBS' THEN '38499999997'
                    ELSE '38499999999'
                END) NCP ,
                '  ' SUF ,
                (CASE
                    WHEN OPERATIONTYPE_CODE IS NOT NULL THEN OPERATIONTYPE_CODE
                    ELSE CONCAT('ERROR: SERVICE_TYPE '
                        ,A.SERVICE_TYPE
                        ,' ABSENT DE LA TABLE DIM.DT_OM_OPERATION_TYPE. EVENT DATE: '
                        , TO_DATE (EVENT_DATE)
                        , ',  ACCOUNT_NUMBER='
                        , ACCOUNT_NUMBER
                    )
                END)    OPE ,
                'AUTO' UTI ,
                (CASE
                    WHEN ACCOUNT_KEY IS NOT NULL THEN ACCOUNT_KEY
                    WHEN ACCOUNT_KEY IS NULL
                    AND    USER_CATEGORY_CODE='SUBS' THEN '55'
                    ELSE '49'
                END) CLC ,
                TO_DATE (EVENT_DATE) DCO ,
                TO_DATE (EVENT_DATE) DVA ,
                AMOUNT MON ,
                SENS SEN ,
                SUBSTR(CONCAT(OM_OPERATION_NAME, '.', ' ', '.chrg'), 1, 30) LIB ,
                CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD(N_ROWNUM, 6, '0')) PIE ,
                ' ' MAR ,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGSA ,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGEM ,
                (CASE
                    WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY
                    ELSE '06815'
                END) AGDE ,
                (CASE
                    WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE
                    ELSE '001'
                END)   DEVC ,
                AMOUNT MCTV ,
                CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO ,
                EVENT_DATE ,
                CURRENT_TIMESTAMP INSERT_DATE
            FROM
            (
                SELECT
                    ROW_NUMBER() OVER(ORDER BY PAYER_USER_ID) N_ROWNUM ,
                    TO_DATE(TRANSACTION_DATE) EVENT_DATE ,
                    SERVICE_TYPE ,
                    PAYER_USER_ID USER_ID ,
                    PAYER_CATEGORY_CODE USER_CATEGORY_CODE ,
                    SERVICE_CHARGE_AMOUNT AMOUNT ,
                    'D' SENS
                FROM CDR.SPARK_IT_OMNY_SERVICES_CHARGES_DETAILS
                WHERE SERVICE_CHARGE_AMOUNT>0
                    AND TO_DATE(TRANSACTION_DATE) >='###SLICE_VALUE###'
                    AND TO_DATE(TRANSACTION_DATE) < DATE_ADD('###SLICE_VALUE###',1)
                UNION
                SELECT
                    ROW_NUMBER() OVER(ORDER BY PAYER_USER_ID ) N_ROWNUM ,
                    TO_DATE(TRANSACTION_DATE) EVENT_DATE ,
                    SERVICE_TYPE ,
                    PAYER_USER_ID USER_ID ,
                    PAYEE_CATEGORY_CODE USER_CATEGORY_CODE , 
                    SERVICE_CHARGE_AMOUNT AMOUNT , 
                    'C' SENS 
                FROM CDR.SPARK_IT_OMNY_SERVICES_CHARGES_DETAILS
                WHERE SERVICE_CHARGE_AMOUNT>0
                    AND TO_DATE(TRANSACTION_DATE) >='###SLICE_VALUE###'
                    AND TO_DATE(TRANSACTION_DATE) < DATE_ADD('###SLICE_VALUE###',1)
            ) A 
            LEFT JOIN DIM.DT_OM_BANK_PARTNER_ACCOUNT B 
            ON (A.USER_ID=B.ACCOUNT_ID) 
            LEFT JOIN DIM.DT_OM_OPERATION_TYPE C 
            ON (A.SERVICE_TYPE = C.SERVICE_TYPE)
        ) Results
    ) T0
) T
