INSERT INTO CDR.SPARK_IT_BALANCE_AGEE PARTITION (AS_OF_DATE)
SELECT
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
CODE_CLIENT,
ACCOUNT_NUMBER,
ACCOUNT_NAME,
CATEG,
PAYMENT_TERM,
STATUT,
NOM,
BALANCE,
DATE_DERNIERE_FACTURE,
0JRS,
30JRS,
60JRS,
90JRS,
120JRS,
150JRS,
180JRS,
360JRS,
720JRS,
1080JRS,
1440JRS,
1800JRS,
PLUS_1800JRS,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
FROM_UNIXTIME(UNIX_TIMESTAMP(AS_OF_DATE,'yyyyMMdd'),'yyyy-MM-dd')AS_OF_DATE
FROM CDR.TT_BALANCE_AGEE C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_BALANCE_AGEE)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL