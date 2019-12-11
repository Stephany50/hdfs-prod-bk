INSERT INTO CDR.IT_CONT
SELECT

Subs_id  ,
Customer_id  ,
Subscriber_type   ,
Default_price_plan_ID  ,
ACCNBR  ,
ICCID  ,
IMSI  ,
PROD_STATE  ,
block_reason  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(update_date, 'dd/MM/yy hh:mm:ss')) update_date  ,
CUID  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(activation_date, 'dd/MM/yy hh:mm:ss')) activation_date  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_MAJ, 'dd/MM/yy hh:mm:ss')) DATE_MAJ,
LOGIN_UTILISATEUR_MAJ,

ORIGINAL_FILE_NAME  ,
ORIGINAL_FILE_SIZE  ,
ORIGINAL_FILE_LINE_COUNT ,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_CONT  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_CONT) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;