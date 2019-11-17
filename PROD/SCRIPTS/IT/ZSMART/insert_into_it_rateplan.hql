INSERT INTO CDR.IT_RATEPLAN
SELECT

Default_price_plan_ID   ,
DESCRIPTION   ,
ORIGINAL_FILE_NAME   ,
ORIGINAL_FILE_SIZE   ,
ORIGINAL_FILE_LINE_COUNT   ,

CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -25, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_RATEPLAN  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_RATEPLAN) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;