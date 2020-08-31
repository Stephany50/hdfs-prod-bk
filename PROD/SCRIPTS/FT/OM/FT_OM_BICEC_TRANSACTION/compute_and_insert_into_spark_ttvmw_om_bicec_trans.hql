INSERT INTO
   MON.SPARK_TTVMW_OM_BICEC_TRANS
   SELECT
      AGE,
      DEV,
      CHA,
      NCP,
      SUF,
      OPE,
      UTI,
      CLC,
      DCO,
      DVA,
      MON,
      SEN,
      LIB,
      PIE,
      MAR,
      AGSA,
      AGEM,
      AGDE,
      DEVC,
      MCTV,
      PIEO,
      NULL USER_ID,
      NULL REMARK,
      CURRENT_TIMESTAMP INSERT_DATE,
      EVENT_DATE
   FROM
      (
         SELECT
            (
            CASE
               WHEN
                  ACCOUNT_AGENCY IS NOT NULL
               THEN
                  ACCOUNT_AGENCY
               ELSE
                  '06815'
            END
            ) AGE ,
            (
               CASE
                  WHEN
                     MONEY_CODE IS NOT NULL
                  THEN
                     MONEY_CODE
                  ELSE
                     '001'
               END
            )
            DEV ,
            (
               CASE
                  WHEN
                     FINANCIAL_CHAPTER IS NOT NULL
                  THEN
                     FINANCIAL_CHAPTER
                  ELSE
                     '384999'
               END
            )
            CHA ,
            (
               CASE
                  WHEN
                     ACCOUNT_NUMBER IS NOT NULL
                  THEN
                     ACCOUNT_NUMBER
                  WHEN
                     ACCOUNT_NUMBER IS NULL
                     AND USER_CATEGORY_CODE = 'SUBS'
                  THEN
                     '38499999997'
                  ELSE
                     '38499999999'
               END
            )
            NCP , '  ' SUF ,
            (
               CASE
                  WHEN
                     OPERATIONTYPE_CODE IS NOT NULL
                  THEN
                     OPERATIONTYPE_CODE
                  ELSE
                     ''
               END
            )
            OPE , 'AUTO' UTI ,
            (
               CASE
                  WHEN
                     ACCOUNT_KEY IS NOT NULL
                  THEN
                     ACCOUNT_KEY
                  WHEN
                     ACCOUNT_KEY IS NULL
                     AND USER_CATEGORY_CODE = 'SUBS'
                  THEN
                     '55'
                  ELSE
                     '49'
               END
            )
            CLC ,
			TO_DATE (EVENT_DATE) DCO ,
			TO_DATE (EVENT_DATE) DVA ,
			AMOUNT MON ,
			SENS SEN ,
			SUBSTR (CONCAT(OM_OPERATION_NAME , '.' , ' ' , '.amnt'), 1, 30) LIB ,
			CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIE ,
			' ' MAR ,
            (
               CASE
                  WHEN
                     ACCOUNT_AGENCY IS NOT NULL
                  THEN
                     ACCOUNT_AGENCY
                  ELSE
                     '06815'
               END
            )
            AGSA ,
            (
               CASE
                  WHEN
                     ACCOUNT_AGENCY IS NOT NULL
                  THEN
                     ACCOUNT_AGENCY
                  ELSE
                     '06815'
               END
            )
            AGEM ,
            (
               CASE
                  WHEN
                     ACCOUNT_AGENCY IS NOT NULL
                  THEN
                     ACCOUNT_AGENCY
                  ELSE
                     '06815'
               END
            )
            AGDE ,
            (
               CASE
                  WHEN
                     MONEY_CODE IS NOT NULL
                  THEN
                     MONEY_CODE
                  ELSE
                     '001'
               END
            )
            DEVC ,
			AMOUNT MCTV ,
			CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO ,
			EVENT_DATE ,
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
                        TRANSFER_DATETIME EVENT_DATE,
                        SERVICE_TYPE,
                        SENDER_USER_ID USER_ID,
                        SENDER_CATEGORY_CODE USER_CATEGORY_CODE,
                        TRANSACTION_AMOUNT AMOUNT,
                        'D' SENS
                     FROM
                        CDR.SPARK_IT_OMNY_TRANSACTIONS
                     WHERE
                        (
                           TRANSFER_STATUS = 'TS'
                           OR
                           (
                              TRANSFER_STATUS = 'TF'
                              AND RECONCILIATION_BY IS NOT NULL
                           )
                           OR
                           (
                              TRANSFER_STATUS = 'TF'
                              AND SENDER_PRE_BAL <> SENDER_POST_BAL
                           )
                        )
                        AND TRANSFER_DATETIME = '###SLICE_VALUE###'
                     UNION
                     SELECT
                        ROW_NUMBER() OVER(ORDER BY RECEIVER_USER_ID) N_ROWNUM,
                        TRANSFER_DATETIME EVENT_DATE,
                        SERVICE_TYPE,
                        RECEIVER_USER_ID USER_ID,
                        RECEIVER_CATEGORY_CODE USER_CATEGORY_CODE,
                        TRANSACTION_AMOUNT AMOUNT,
                        'C' SENS
                     FROM
                        CDR.SPARK_IT_OMNY_TRANSACTIONS
                     WHERE
                        (
                           TRANSFER_STATUS = 'TS'
                           OR
                           (
                              TRANSFER_STATUS = 'TF'
                              AND RECONCILIATION_BY IS NOT NULL
                           )
                           OR
                           (
                              TRANSFER_STATUS = 'TF'
                              AND SENDER_PRE_BAL <> SENDER_POST_BAL
                           )
                        )
                        AND TRANSFER_DATETIME = '###SLICE_VALUE###'
                  )
                  A
                  LEFT JOIN
                     DIM.DT_OM_OPERATION_TYPE C
                     ON (A.SERVICE_TYPE = C.SERVICE_TYPE)
            )
            A
            LEFT JOIN
               DIM.DT_OM_BANK_PARTNER_ACCOUNT B
               ON (A.USER_ID = B.ACCOUNT_ID)
            UNION ALL
            SELECT
(
               CASE
                  WHEN
                     ACCOUNT_AGENCY IS NOT NULL
                  THEN
                     ACCOUNT_AGENCY
                  ELSE
                     '06815'
               END
) AGE ,
               (
                  CASE
                     WHEN
                        MONEY_CODE IS NOT NULL
                     THEN
                        MONEY_CODE
                     ELSE
                        '001'
                  END
               )
               DEV ,
               (
                  CASE
                     WHEN
                        FINANCIAL_CHAPTER IS NOT NULL
                     THEN
                        FINANCIAL_CHAPTER
                     ELSE
                        '384999'
                  END
               )
               CHA ,
               (
                  CASE
                     WHEN
                        ACCOUNT_NUMBER IS NOT NULL
                     THEN
                        ACCOUNT_NUMBER
                     WHEN
                        ACCOUNT_NUMBER IS NULL
                        AND USER_CATEGORY_CODE = 'SUBS'
                     THEN
                        '38499999997'
                     ELSE
                        '38499999999'
                  END
               )
               NCP , '  ' SUF ,
               (
                  CASE
                     WHEN
                        OPERATIONTYPE_CODE IS NOT NULL
                     THEN
                        OPERATIONTYPE_CODE
                     ELSE
                        ''
                  END
               )
               OPE , 'AUTO' UTI ,
               (
                  CASE
                     WHEN
                        ACCOUNT_KEY IS NOT NULL
                     THEN
                        ACCOUNT_KEY
                     WHEN
                        ACCOUNT_KEY IS NULL
                        AND USER_CATEGORY_CODE = 'SUBS'
                     THEN
                        '55'
                     ELSE
                        '49'
                  END
               )
               CLC ,
			   TO_DATE (EVENT_DATE) DCO ,
			   TO_DATE (EVENT_DATE) DVA ,
			   AMOUNT MON ,
			   SENS SEN ,
			   SUBSTR (CONCAT(OM_OPERATION_NAME , '.' , ' ' , '.cmms'), 1, 30) LIB ,
			   CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIE ,
			   ' ' MAR ,
               (
                  CASE
                     WHEN
                        ACCOUNT_AGENCY IS NOT NULL
                     THEN
                        ACCOUNT_AGENCY
                     ELSE
                        '06815'
                  END
               )
               AGSA ,
               (
                  CASE
                     WHEN
                        ACCOUNT_AGENCY IS NOT NULL
                     THEN
                        ACCOUNT_AGENCY
                     ELSE
                        '06815'
                  END
               )
               AGEM ,
               (
                  CASE
                     WHEN
                        ACCOUNT_AGENCY IS NOT NULL
                     THEN
                        ACCOUNT_AGENCY
                     ELSE
                        '06815'
                  END
               )
               AGDE ,
               (
                  CASE
                     WHEN
                        MONEY_CODE IS NOT NULL
                     THEN
                        MONEY_CODE
                     ELSE
                        '001'
                  END
               )
               DEVC ,
			   AMOUNT MCTV ,
			   CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO ,
			   EVENT_DATE ,
			   CURRENT_TIMESTAMP INSERT_DATE
            FROM
               (
                  SELECT
                     ROW_NUMBER() OVER(ORDER BY PAYER_USER_ID ) N_ROWNUM,
                     TRANSACTION_DATE EVENT_DATE,
                     SERVICE_TYPE,
                     PAYER_USER_ID USER_ID,
                     PAYER_CATEGORY_CODE USER_CATEGORY_CODE,
                     COMMISSION_AMOUNT AMOUNT,
                     'D' SENS
                  FROM
                     CDR.SPARK_IT_OMNY_COMMISSION_DETAILS
                  WHERE
                     COMMISSION_AMOUNT > 0
                     AND TRANSACTION_DATE = '###SLICE_VALUE###'
                  UNION
                  SELECT
                     ROW_NUMBER() OVER(ORDER BY PAYEE_USER_ID) N_ROWNUM,
                     TRANSACTION_DATE EVENT_DATE,
                     SERVICE_TYPE,
                     PAYEE_USER_ID USER_ID,
                     PAYEE_CATEGORY_CODE USER_CATEGORY_CODE,
                     COMMISSION_AMOUNT AMOUNT,
                     'C' SENS
                  FROM
                     CDR.SPARK_IT_OMNY_COMMISSION_DETAILS
                  WHERE
                     COMMISSION_AMOUNT > 0
                     AND TRANSACTION_DATE = '###SLICE_VALUE###'
               )
               A
               LEFT JOIN
                  DIM.DT_OM_BANK_PARTNER_ACCOUNT B
                  ON (A.USER_ID = B.ACCOUNT_ID)
               LEFT JOIN
                  DIM.DT_OM_OPERATION_TYPE C
                  ON (A.SERVICE_TYPE = C.SERVICE_TYPE)
               UNION ALL
               SELECT
                  (
                  CASE
                     WHEN
                        ACCOUNT_AGENCY IS NOT NULL
                     THEN
                        ACCOUNT_AGENCY
                     ELSE
                        '06815'
                  END
                  ) AGE ,
                  (
                     CASE
                        WHEN
                           MONEY_CODE IS NOT NULL
                        THEN
                           MONEY_CODE
                        ELSE
                           '001'
                     END
                  )
                  DEV ,
                  (
                     CASE
                        WHEN
                           FINANCIAL_CHAPTER IS NOT NULL
                        THEN
                           FINANCIAL_CHAPTER
                        ELSE
                           '384999'
                     END
                  )
                  CHA ,
                  (
                     CASE
                        WHEN
                           ACCOUNT_NUMBER IS NOT NULL
                        THEN
                           ACCOUNT_NUMBER
                        WHEN
                           ACCOUNT_NUMBER IS NULL
                           AND USER_CATEGORY_CODE = 'SUBS'
                        THEN
                           '38499999997'
                        ELSE
                           '38499999999'
                     END
                  )
                  NCP , '  ' SUF ,
                  (
                     CASE
                        WHEN
                           OPERATIONTYPE_CODE IS NOT NULL
                        THEN
                           OPERATIONTYPE_CODE
                        ELSE
                           ''
                     END
                  )
                  OPE , 'AUTO' UTI ,
                  (
                     CASE
                        WHEN
                           ACCOUNT_KEY IS NOT NULL
                        THEN
                           ACCOUNT_KEY
                        WHEN
                           ACCOUNT_KEY IS NULL
                           AND USER_CATEGORY_CODE = 'SUBS'
                        THEN
                           '55'
                        ELSE
                           '49'
                     END
                  )
                  CLC ,
				  TO_DATE (EVENT_DATE) DCO ,
				  TO_DATE (EVENT_DATE) DVA ,
				  AMOUNT MON ,
				  SENS SEN ,
				  SUBSTR (CONCAT(OM_OPERATION_NAME , '.' , ' ' , '.chrg'), 1, 30) LIB ,
				  CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIE ,
				  ' ' MAR ,
                  (
                     CASE
                        WHEN
                           ACCOUNT_AGENCY IS NOT NULL
                        THEN
                           ACCOUNT_AGENCY
                        ELSE
                           '06815'
                     END
                  )
                  AGSA ,
                  (
                     CASE
                        WHEN
                           ACCOUNT_AGENCY IS NOT NULL
                        THEN
                           ACCOUNT_AGENCY
                        ELSE
                           '06815'
                     END
                  )
                  AGEM ,
                  (
                     CASE
                        WHEN
                           ACCOUNT_AGENCY IS NOT NULL
                        THEN
                           ACCOUNT_AGENCY
                        ELSE
                           '06815'
                     END
                  )
                  AGDE ,
                  (
                     CASE
                        WHEN
                           MONEY_CODE IS NOT NULL
                        THEN
                           MONEY_CODE
                        ELSE
                           '001'
                     END
                  )
                  DEVC ,
				  AMOUNT MCTV ,
				  CONCAT('OM', LPAD (OM_OPERATION_CODE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO ,
				  EVENT_DATE ,
				  CURRENT_TIMESTAMP INSERT_DATE
               FROM
                  (
                     SELECT
                        ROW_NUMBER() OVER(ORDER BY PAYER_USER_ID) N_ROWNUM,
                        TRANSACTION_DATE EVENT_DATE,
                        SERVICE_TYPE,
                        PAYER_USER_ID USER_ID,
                        PAYER_CATEGORY_CODE USER_CATEGORY_CODE,
                        SERVICE_CHARGE_AMOUNT AMOUNT,
                        'D' SENS
                     FROM
                        CDR.SPARK_IT_OMNY_SERVICES_CHARGES_DETAILS
                     WHERE
                        SERVICE_CHARGE_AMOUNT > 0
                        AND TRANSACTION_DATE = '###SLICE_VALUE###'
                     UNION
                     SELECT
                        ROW_NUMBER() OVER(ORDER BY PAYER_USER_ID) N_ROWNUM,
                        TRANSACTION_DATE EVENT_DATE,
                        SERVICE_TYPE,
                        PAYEE_USER_ID USER_ID,
                        PAYEE_CATEGORY_CODE USER_CATEGORY_CODE,
                        SERVICE_CHARGE_AMOUNT AMOUNT,
                        'C' SENS
                     FROM
                        CDR.SPARK_IT_OMNY_SERVICES_CHARGES_DETAILS
                     WHERE
                        SERVICE_CHARGE_AMOUNT > 0
                        AND TRANSACTION_DATE = '###SLICE_VALUE###'
                  )
                  A
                  LEFT JOIN
                     DIM.DT_OM_BANK_PARTNER_ACCOUNT B
                     ON (A.USER_ID = B.ACCOUNT_ID)
                  LEFT JOIN
                     DIM.DT_OM_OPERATION_TYPE C
                     ON (A.SERVICE_TYPE = C.SERVICE_TYPE)
      )
      Results

