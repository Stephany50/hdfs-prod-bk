INSERT INTO CDR.spark_it_bdi_hlr
select
IMSI  ,
MSISDN ,
ODBIC,
ODBOC,
ODBPLMN1,
ODBPLMN2,
ODBPLMN3,
ODBPLMN4,
ODBROAM,
ODBDECT,
ODBMECT,
ODBINFO,
ODBECT,
ODBPOS,
ODBPOSTYPE,
ODBRCF,
ODBENTER ,
ODBSS,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM cdr.tt_bdi_hlr C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.spark_it_bdi_hlr where  ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,2) AND CURRENT_DATE)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL