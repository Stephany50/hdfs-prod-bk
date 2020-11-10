SELECT
from_unixtime(unix_timestamp(PERIODE_REFERENCE, 'yyyy-MM-dd'), 'dd/MM/yyyy') PERIODE_REFERENCE,  
PARTNER_FIRST_NAME,
PARTNER_LAST_NAME,
ACCOUNT_AGENCY,
ACCOUNT_NUMBER,
ACCOUNT_KEY,
replace(regexp_replace(BALANCE_DEBUT, '\\.*(0)+$', ''), '.', ',') AS BALANCE_DEBUT,
replace(regexp_replace(CREDIT, '\\.*(0)+$', ''), '.', ',') AS CREDIT,
replace(regexp_replace(DEBIT, '\\.*(0)+$', ''), '.', ',') AS DEBIT,
replace(regexp_replace(BALANCE_ESTIME_FIN, '\\.*(0)+$', ''), '.', ',') AS BALANCE_ESTIME_FIN

FROM (
 SELECT
    PERIODE_REFERENCE
    ,PARTNER_FIRST_NAME
    ,PARTNER_LAST_NAME
    ,ACCOUNT_AGENCY
    ,ACCOUNT_NUMBER
    ,ACCOUNT_KEY
    ,SUM(BAL_DEBUT) BALANCE_DEBUT
    ,SUM(CREDIT) CREDIT
    ,SUM(DEBIT) DEBIT
    ,SUM(BAL_ESTIME) BALANCE_ESTIME_FIN
    ,CURRENT_TIMESTAMP() INSERT_DATE
    ,'###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        '###SLICE_VALUE###' PERIODE_REFERENCE
        ,A01.PARTNER_FIRST_NAME
        ,A01.PARTNER_LAST_NAME
        ,A01.ACCOUNT_AGENCY
        ,A01.NCP ACCOUNT_NUMBER
        ,A01.ACCOUNT_KEY
        ,A01.ACCOUNT_FULLNUMBER
        ,NVL(A01.BAL_DEBUT,0) BAL_DEBUT
        ,NVL(A00.CREDIT,0) CREDIT
        ,NVL(A00.DEBIT,0) DEBIT
        ,NVL(A01.BAL_FIN,0) BAL_FIN
        ,NVL(A01.BAL_DEBUT, 0)+NVL(A00.CREDIT, 0)-NVL(A00.DEBIT, 0) BAL_ESTIME
        ,NVL(A01.BAL_DEBUT, 0)+NVL(A00.CREDIT, 0)-NVL(A00.DEBIT, 0)-NVL(A01.BAL_FIN, 0) ECART
    FROM
    (
        SELECT
            NCP
            ,SUM(CASE WHEN SEN='D' THEN nvl(MON,0) ELSE 0 END) DEBIT
            ,SUM(CASE WHEN SEN='C' THEN nvl(MON,0) ELSE 0 END) CREDIT
        FROM MON.SPARK_FT_OM_BICEC_TRANSACTION
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND MON>0
        GROUP BY NCP
    ) A00
    RIGHT JOIN
    (
        SELECT
            PARTNER_FIRST_NAME
            ,PARTNER_LAST_NAME
            ,ACCOUNT_AGENCY
            ,ACCOUNT_KEY
            ,ACCOUNT_FULLNUMBER
            ,NCP
            ,SUM(NVL(BAL_DEBUT,0)) BAL_DEBUT
            ,SUM(NVL(BAL_FIN,0)) BAL_FIN
        FROM
        (
            SELECT
                A0100.PARTNER_FIRST_NAME
                ,A0100.PARTNER_LAST_NAME
                ,A0100.ACCOUNT_AGENCY
                ,A0100.ACCOUNT_KEY
                ,A0100.ACCOUNT_FULLNUMBER
                ,A0100.ACCOUNT_NUMBER NCP
                ,A0101.BALANCE BAL_DEBUT
                ,A0102.BALANCE BAL_FIN
            FROM
            DIM.SPARK_DT_OM_BANK_PARTNER_ACCOUNT A0100
            LEFT JOIN
            (
                SELECT *
                FROM MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT
                WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1)
            ) A0101
            ON A0100.ACCOUNT_ID = A0101.USER_ID
            LEFT JOIN
            (
                SELECT *
                FROM MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT
                WHERE EVENT_DATE = '###SLICE_VALUE###'
            ) A0102
            ON A0100.ACCOUNT_ID = A0102.USER_ID
        ) A010
        GROUP BY PARTNER_FIRST_NAME
            ,PARTNER_LAST_NAME
            ,ACCOUNT_AGENCY
            ,ACCOUNT_KEY
            ,ACCOUNT_FULLNUMBER
            ,NCP
    ) A01
    ON A01.NCP = A00.NCP
) A0
GROUP BY PERIODE_REFERENCE,
    PARTNER_FIRST_NAME,
    PARTNER_LAST_NAME,
    ACCOUNT_AGENCY,
    ACCOUNT_NUMBER,
    ACCOUNT_KEY
) R
WHERE EVENT_DATE = '###SLICE_VALUE###'