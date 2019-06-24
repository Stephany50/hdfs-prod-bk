INSERT INTO CTI.SVI_APPEL
SELECT
  ID_APPEL    ,
DATE_DEBUT_OMS DATE_DEBUT_OMS_NQ ,
DATE_DEBUT_SVI,
TYPE_HANGUP ,
DATE_HANGUP,
LANGUE,
SEGMENT_CLIENT,
MSISDN,
SERVICE,
ETAT_BASE,
NUMERO_TRANSFERT,
TYPE_APPEL,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(DATE_DEBUT_OMS) DATE_DEBUT_OMS
FROM CTI.TT_SVI_APPEL C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.SVI_APPEL)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;