INSERT INTO SPOOL.SPOOL_OM_BICEC_TRANS_RECONCILIATION

SELECT

PERIODE_REFERENCE,
PARTNER_FIRST_NAME,
PARTNER_LAST_NAME,
ACCOUNT_AGENCY,
ACCOUNT_NUMBER,
ACCOUNT_KEY,
SUM(BAL_DEBUT) SUM_BAL_DEBUT,
SUM(CREDIT) SUM_CREDIT,
SUM(DEBIT) SUM_DEBIT,
SUM(BAL_ESTIME) SUM_BAL_ESTIME,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_DATE

FROM

        (SELECT

        '2020-02-02' AS PERIODE_REFERENCE,
        B.PARTNER_FIRST_NAME,
        B.PARTNER_LAST_NAME,
        B.ACCOUNT_AGENCY,
        B.ncp ACCOUNT_NUMBER,
        B.ACCOUNT_KEY,
        B.ACCOUNT_FULLNUMBER,
        NVL(B.BAL_DEBUT,0) BAL_DEBUT,
        NVL(A.CREDIT,0) CREDIT,
        NVL(A.DEBIT,0) DEBIT,
        NVL(B.BAL_FIN,0) BAL_FIN,
        NVL(B.BAL_DEBUT, 0)+NVL(A.CREDIT, 0)-NVL(A.DEBIT, 0) BAL_ESTIME,
        NVL(B.BAL_DEBUT, 0)+NVL(A.CREDIT, 0)-NVL(A.DEBIT, 0)-NVL(B.BAL_FIN, 0) ECART
        FROM
                (
                SELECT PARTNER_FIRST_NAME,
                PARTNER_LAST_NAME,
                ACCOUNT_AGENCY,
                ACCOUNT_KEY,
                ACCOUNT_FULLNUMBER,
                NCP,
                SUM(nvl(BAL_DEBUT,0)) BAL_DEBUT,
                SUM(nvl(BAL_FIN,0)) BAL_FIN
                FROM
                        (
                        SELECT
                        C.PARTNER_FIRST_NAME AS PARTNER_FIRST_NAME ,
                        C.PARTNER_LAST_NAME AS PARTNER_LAST_NAME,
                        C.ACCOUNT_AGENCY AS ACCOUNT_AGENCY,
                        C.ACCOUNT_KEY AS ACCOUNT_KEY,
                        C.ACCOUNT_FULLNUMBER  AS ACCOUNT_FULLNUMBER,
                        C.ACCOUNT_NUMBER NCP,
                        A.BALANCE BAL_DEBUT,
                        B.BALANCE BAL_FIN
                        FROM
                                (SELECT * FROM DIM.SPARK_DT_OM_BANK_PARTNER_ACCOUNT) C
                                LEFT JOIN
                                (SELECT *
                                FROM
                                MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT
                                WHERE EVENT_DATE=DATE_SUB('2020-02-02', 1)) A
                                 ON C.ACCOUNT_ID=A.USER_ID
                                LEFT JOIN
                                (SELECT *
                                FROM
                                MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT
                                WHERE EVENT_DATE='2020-02-02') B
                                ON C.ACCOUNT_ID=B.USER_ID
                        ) TTT
                GROUP BY PARTNER_FIRST_NAME, PARTNER_LAST_NAME, ACCOUNT_AGENCY, ACCOUNT_KEY, ACCOUNT_FULLNUMBER, NCP
                ) B
                LEFT JOIN
                (
                SELECT
                NCP,
                SUM(CASE WHEN SEN='D' THEN nvl(MON,0) ELSE 0 END ) DEBIT,
                SUM(CASE WHEN SEN='C' THEN nvl(MON,0) ELSE 0 END ) CREDIT
                FROM MON.SPARK_FT_OM_BICEC_TRANSACTION
                WHERE EVENT_DATE = '2020-02-02' AND MON>0
                GROUP BY NCP
                ) A
                ON B.NCP=A.NCP
        ) TTT
        GROUP BY PERIODE_REFERENCE, PARTNER_FIRST_NAME, PARTNER_LAST_NAME, ACCOUNT_AGENCY, ACCOUNT_NUMBER, ACCOUNT_KEY
