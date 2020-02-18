INSERT INTO SPOOL.SPOOL_ECRITURE_APGL
SELECT
    B.ACCOUNTING_TYPE,
    B.ACCOUNT_NO,
    A.TRANSACTION_DATE DOCUMENT_DATE,
    A.TRANSACTION_DATE POSTING_DATE,
    NULL DOCUMENT_NO,
    CASE B.TRANSACTION_TYPE WHEN NULL THEN NULL ELSE B.TRANSACTION_TYPE ||'-'|| B.SENDER_USER_TYPE ||'-'|| B.RECEIVER_USER_TYPE END DESCRIPTION,
    NULL TRANSACTION_LINE_DESCRIPTION,
    NULL CURRENCY_CODE,
    (
        CASE B.TRANSACTION_AMOUNT WHEN '0' THEN 0 WHEN 'Credit' THEN -A.TRANSACTION_AMOUNT WHEN 'Debit' THEN A.TRANSACTION_AMOUNT ELSE 0 END
        +   CASE B.COMMISSION_GROSSISTE WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_GROSSISTE WHEN 'Debit' THEN A.COMMISSION_GROSSISTE ELSE 0 END
        +   CASE B.COMMISSION_AGENT WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_AGENT WHEN 'Debit' THEN A.COMMISSION_AGENT ELSE 0 END
        +   CASE B.COMMISSION_OCA WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_OCA WHEN 'Debit' THEN A.COMMISSION_OCA ELSE 0 END
        +   CASE B.COMMISSION_AUTRE WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_AUTRE WHEN 'Debit' THEN A.COMMISSION_AUTRE ELSE 0 END
        +   CASE B.SERVICE_CHARGE_AMOUNT WHEN '0' THEN 0 WHEN 'Credit' THEN -A.SERVICE_CHARGE_AMOUNT WHEN 'Debit' THEN A.SERVICE_CHARGE_AMOUNT ELSE 0 END
    ) AMOUNT,
    (
        CASE B.TRANSACTION_AMOUNT WHEN '0' THEN 0 WHEN 'Credit' THEN -A.TRANSACTION_AMOUNT WHEN 'Debit' THEN A.TRANSACTION_AMOUNT ELSE 0 END
        +   CASE B.COMMISSION_GROSSISTE WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_GROSSISTE WHEN 'Debit' THEN A.COMMISSION_GROSSISTE ELSE 0 END
        +   CASE B.COMMISSION_AGENT WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_AGENT WHEN 'Debit' THEN A.COMMISSION_AGENT ELSE 0 END
        +   CASE B.COMMISSION_OCA WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_OCA WHEN 'Debit' THEN A.COMMISSION_OCA ELSE 0 END
        +   CASE B.COMMISSION_AUTRE WHEN '0' THEN 0 WHEN 'Credit' THEN -A.COMMISSION_AUTRE WHEN 'Debit' THEN A.COMMISSION_AUTRE ELSE 0 END
        +   CASE B.SERVICE_CHARGE_AMOUNT WHEN '0' THEN 0 WHEN 'Credit' THEN -A.SERVICE_CHARGE_AMOUNT WHEN 'Debit' THEN A.SERVICE_CHARGE_AMOUNT ELSE 0 END
    ) AMOUNT_LVC,
    B.CODE_SOUS_COMPTE SOUS_COMPTE,
    B.CODE_ORGANISATION ORGANISATION,
    B.CODE_OFFRE OFFRE,
    B.CODE_PARTENAIRE_CODE PARTNER,
    B.CODE_SITE SITE,
    B.CODE_PROJET PROJECT,
    B.CODE_FLUX FLUX,
    NULL SC8,
    NULL SALESPERSON_CODE,
    NULL DUE_DATE,
    NULL PAYMENT_DISCOUNT,
    NULL PAYMENT_TERMS_CODE,
    NULL EXTERNAL_DOC_NO,
    CURRENT_TIMESTAMP() INSERT_DATE,
    A.TRANSACTION_DATE AS EVENT_DATE
FROM
(
    SELECT
        IT.*,
        B1.SR_USER_TYPE SENDER_USER_TYPE,
        B2.SR_USER_TYPE RECEIVER_USER_TYPE
    FROM
    (
        SELECT
            TRANSACTION_DATE,
            TRANSACTION_TAG,
            TRANSACTION_AMOUNT,
            COMMISSION_GROSSISTE,
            COMMISSION_AGENT,
            COMMISSION_OCA,
            COMMISSION_AUTRE,
            SENDER_CATEGORY_CODE,
            RECEIVER_CATEGORY_CODE,
            SERVICE_CHARGE_AMOUNT
        FROM CDR.SPARK_IT_OM_APGL
        WHERE TRANSACTION_DATE = "###SLICE_VALUE###"
    ) IT
    LEFT JOIN DIM.SPARK_DT_OM_PARTNER_SETUP B1 ON (UPPER(IT.SENDER_CATEGORY_CODE) = UPPER(B1.SR_CATEGORY_CODE))
    LEFT JOIN DIM.SPARK_DT_OM_PARTNER_SETUP B2 ON (UPPER(IT.RECEIVER_CATEGORY_CODE) = UPPER(B2.SR_CATEGORY_CODE))
) A
LEFT JOIN DIM.SPARK_DT_OM_TRANS_SETUP B 
ON (
    A.TRANSACTION_TAG = B.TRANSACTION_TYPE
    AND A.SENDER_USER_TYPE = B.SENDER_USER_TYPE
    AND A.RECEIVER_USER_TYPE = B.RECEIVER_USER_TYPE
)
