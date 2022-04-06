INSERT INTO  CDR.SPARK_IT_RATECHAN
SELECT

    Subs_id
     ,OLD_DEFAULT_PRICE_PLAN_ID
     ,NEW_DEFAULT_PRICE_PLAN_ID
     ,FROM_UNIXTIME(UNIX_TIMESTAMP(Update_date, 'dd/MM/yy hh:mm:ss')) Update_date
     ,CUID
     ,ORIGINAL_FILE_NAME
     ,ORIGINAL_FILE_SIZE
     ,ORIGINAL_FILE_LINE_COUNT
     ,CURRENT_TIMESTAMP() INSERT_DATE
     ,TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_RATECHAN  C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.SPARK_IT_RATECHAN) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL