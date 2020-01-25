--*********************************
--***      MARKETING DATAMART    **
--*********************************
INSERT INTO MON.SPARK_DATAMART_OM_MARKETING
--create table tmp.DATAMART_OM_MARKETING as
SELECT
    C.ACTIF_ID,
    C.MSISDN,
    C.SERVICE_TYPE,
    C.STYLES,
    C.TECHNOLOGY,
    C.PRODUCT_LINE,
    C.PRODUCT,
    C.DETAILS_MARKETING,
    C.DETAILS_CONFOMITY,
    C.BEAC,
    C.VOL,
    C.VAL,
    C.REVENU,
    C.REVENU_INDIRECT,
    C.COMMISSION,
    D.SITE_NAME,
    C.JOUR
FROM
    (
-- CHANNEL ON SENDER MODE
  SELECT
        A.TRANSACTION_DATE JOUR,
        (A.RECEIVER_USER_ID || '_' || A.RECEIVER_MSISDN) ACTIF_ID,
        A.RECEIVER_MSISDN MSISDN,
        A.SERVICE_TYPE,
        B.STYLES,
        B.TECHNOLOGY,
        B.PRODUCT_LINE,
        B.PRODUCT,
        B.DETAILS_MARKETING,
        B.DETAILS_CONFOMITY,
        B.BEAC,
        COUNT(A.TRANSFER_ID) VOL,
        SUM(A.TRANSACTION_AMOUNT) VAL,
        SUM(A.SERVICE_CHARGE_PAID) REVENU,
        SUM(A.COMMISSIONS_RECEIVED) REVENU_INDIRECT,
        SUM(A.COMMISSIONS_PAID) COMMISSION
    FROM CDR.SPARK_IT_OM_TRANSACTIONS A JOIN MON.SPARK_REF_OM_PRODUCTS B
    ON UPPER(A.SENDER_MSISDN)=UPPER(B.MSISDN) AND UPPER(A.SENDER_USER_ID)=UPPER(B.USER_ID)
    WHERE
        A.TRANSFER_STATUS='TS' AND
        A.TRANSACTION_DATE ='2019-11-19' AND
        A.RECEIVER_CATEGORY_CODE='SUBS' AND A.SENDER_CATEGORY_CODE<>'SUBS' AND
        B.REF_DATE='2019-10-17' AND A.SERVICE_TYPE NOT IN ('P2P', 'P2PNONREG')
    GROUP BY A.TRANSACTION_DATE, (A.RECEIVER_USER_ID || '_' || A.RECEIVER_MSISDN), A.RECEIVER_MSISDN,
        A.SERVICE_TYPE, B.STYLES, B.TECHNOLOGY,  B.PRODUCT_LINE, B.PRODUCT, B.DETAILS_MARKETING, B.DETAILS_CONFOMITY, B.BEAC
    UNION ALL
-- CHANNEL ON RECEIVER MODE
    SELECT
        A.TRANSACTION_DATE JOUR,
        (A.SENDER_USER_ID || '_' || A.SENDER_MSISDN) ID_ACTIF,
        A.SENDER_MSISDN MSISDN,
        A.SERVICE_TYPE,
        B.STYLES,
        B.TECHNOLOGY,
        B.PRODUCT_LINE,
        B.PRODUCT,
        B.DETAILS_MARKETING,
        B.DETAILS_CONFOMITY,
        B.BEAC,
        COUNT(A.TRANSFER_ID) VOL,
        SUM(A.TRANSACTION_AMOUNT) VAL,
        SUM(A.SERVICE_CHARGE_PAID) REVENU,
        SUM(A.COMMISSIONS_RECEIVED) REVENU_INDIRECT,
        SUM(A.COMMISSIONS_PAID) COMMISSION
    FROM CDR.SPARK_IT_OM_TRANSACTIONS A JOIN MON.SPARK_REF_OM_PRODUCTS B
    ON UPPER(A.RECEIVER_MSISDN)=UPPER(B.MSISDN) AND UPPER(A.RECEIVER_USER_ID)=UPPER(B.USER_ID)
    WHERE
        A.TRANSFER_STATUS='TS' AND
        A.TRANSACTION_DATE ='2019-11-19' AND
        A.SENDER_CATEGORY_CODE='SUBS' AND A.RECEIVER_CATEGORY_CODE<>'SUBS' AND
        B.REF_DATE='2019-10-17' AND A.SERVICE_TYPE NOT IN ('P2P', 'P2PNONREG')
    GROUP BY A.TRANSACTION_DATE, (A.SENDER_USER_ID || '_' || A.SENDER_MSISDN), A.SENDER_MSISDN, A.SERVICE_TYPE, B.STYLES,
    B.TECHNOLOGY, B.PRODUCT_LINE, B.PRODUCT, B.DETAILS_MARKETING, B.DETAILS_CONFOMITY, B.BEAC
    UNION ALL
-- TNO
    SELECT
        A.TRANSACTION_DATE JOUR,
        (A.SENDER_USER_ID || '_' || A.SENDER_MSISDN) ID_ACTIF,
        A.SENDER_MSISDN MSISDN,
        A.SERVICE_TYPE,
    'TRANSFERT' STYLES,
        'USSD' TECHNOLOGY,
        'TRANSFERT' PRODUCT_LINE,
        'TNO' PRODUCT,
        (CASE WHEN TNO_MSISDN IS NULL THEN 'TNO_FREE_DEST' ELSE 'TNO_DEST' END) DETAILS_MARKETING,
        'TNO' DETAILS_CONFOMITY,
        'TRANSFERT' BEAC,
        COUNT(A.TRANSFER_ID) VOL,
        SUM(A.TRANSACTION_AMOUNT) VAL,
        SUM(A.SERVICE_CHARGE_PAID) REVENU,
        SUM(A.COMMISSIONS_RECEIVED) REVENU_INDIRECT,
        SUM(A.COMMISSIONS_PAID) COMMISSION
    FROM CDR.SPARK_IT_OM_TRANSACTIONS A
    WHERE
        A.TRANSFER_STATUS='TS' AND
        A.TRANSACTION_DATE ='2019-11-19' AND
        A.SENDER_CATEGORY_CODE='SUBS' AND
        A.SERVICE_TYPE IN ('P2PNONREG')
    GROUP BY
        A.TRANSACTION_DATE,
        (A.SENDER_USER_ID || '_' || A.SENDER_MSISDN),
        A.SENDER_MSISDN,
        A.SERVICE_TYPE,
        (CASE WHEN TNO_MSISDN IS NULL THEN 'TNO_FREE_DEST' ELSE 'TNO_DEST' END)
    UNION ALL
-- P2P SENDER MODE
    SELECT
        A.TRANSACTION_DATE JOUR,
        (A.SENDER_USER_ID || '_' || A.SENDER_MSISDN) ID_ACTIF,
        A.SENDER_MSISDN MSISDN,
        A.SERVICE_TYPE,
    'TRANSFERT' STYLES,
        'USSD' TECHNOLOGY,
        'TRANSFERT' PRODUCT_LINE,
        'P2P' PRODUCT,
        (CASE WHEN RECEIVER_CATEGORY_CODE='SUBS' THEN 'SENDER_REC_CONNU' ELSE 'SENDER_REC_INCONNU' END) DETAILS_MARKETING,
        (CASE WHEN RECEIVER_CATEGORY_CODE='SUBS' THEN 'SENDER_REC_CONNU' ELSE 'SENDER_REC_INCONNU' END) DETAILS_CONFOMITY,
        'TRANSFERT' BEAC,
        COUNT(A.TRANSFER_ID) VOL,
        SUM(A.TRANSACTION_AMOUNT) VAL,
        SUM(A.SERVICE_CHARGE_PAID) REVENU,
        SUM(A.COMMISSIONS_RECEIVED) REVENU_INDIRECT,
        SUM(A.COMMISSIONS_PAID) COMMISSION
    FROM CDR.SPARK_IT_OM_TRANSACTIONS A
    WHERE
        A.TRANSFER_STATUS='TS' AND
        A.TRANSACTION_DATE ='2019-11-19' AND
        A.SENDER_CATEGORY_CODE='SUBS' AND
        A.SERVICE_TYPE IN ('P2P','OTF') --AND
        --RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM MSISDN_B2W UNION SELECT MSISDN FROM OM_TEST_MSISDNS)
    GROUP BY
        A.TRANSACTION_DATE,
        (A.SENDER_USER_ID || '_' || A.SENDER_MSISDN),
        A.SENDER_MSISDN,
        A.SERVICE_TYPE,
        (CASE WHEN RECEIVER_CATEGORY_CODE='SUBS' THEN 'SENDER_REC_CONNU' ELSE 'SENDER_REC_INCONNU' END)
    UNION ALL
-- RECEIVER CONNU
    SELECT
        A.TRANSACTION_DATE JOUR,
        (A.RECEIVER_USER_ID || '_' || A.RECEIVER_MSISDN) ID_ACTIF,
        A.RECEIVER_MSISDN MSISDN,
        A.SERVICE_TYPE,
    'TRANSFERT' STYLES,
        'USSD' TECHNOLOGY,
        'TRANSFERT' PRODUCT_LINE,
        'P2P' PRODUCT,
        (CASE WHEN SENDER_CATEGORY_CODE='SUBS' THEN 'REC_SENDER_CONNU' ELSE 'REC_SENDER_INCONNU' END) DETAILS_MARKETING,
        (CASE WHEN SENDER_CATEGORY_CODE='SUBS' THEN 'REC_SENDER_CONNU' ELSE 'REC_SENDER_INCONNU' END) DETAILS_CONFOMITY,
        'TRANSFERT' BEAC,
        COUNT(A.TRANSFER_ID) VOL,
        SUM(A.TRANSACTION_AMOUNT) VAL,
        SUM(A.SERVICE_CHARGE_PAID) REVENU,
        SUM(A.COMMISSIONS_RECEIVED) REVENU_INDIRECT,
        SUM(A.COMMISSIONS_PAID) COMMISSION
    FROM CDR.SPARK_IT_OM_TRANSACTIONS A
    WHERE
        A.TRANSFER_STATUS='TS' AND
        A.TRANSACTION_DATE ='2019-11-19' AND
        A.RECEIVER_CATEGORY_CODE='SUBS' AND
        A.SERVICE_TYPE IN ('P2P','OTF') --AND
        --RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM MSISDN_B2W UNION SELECT MSISDN FROM OM_TEST_MSISDNS)
    GROUP BY
        A.TRANSACTION_DATE,
        (A.RECEIVER_USER_ID || '_' || A.RECEIVER_MSISDN),
        A.RECEIVER_MSISDN,
        A.SERVICE_TYPE,
        (CASE WHEN SENDER_CATEGORY_CODE='SUBS' THEN 'REC_SENDER_CONNU' ELSE 'REC_SENDER_INCONNU' END)
) C
LEFT JOIN MON.LOC_OM_H18  D ON C.MSISDN=D.MSISDN