INSERT INTO CTI.IT_IRF_USER_DATA_CUST PARTITION (EVENT_DATE,FILE_DATE)
SELECT
FROM_UNIXTIME(UNIX_TIMESTAMP(EVENT_TIME, 'dd/MM/yyyy HH:mm:ss')) EVENT_TIME ,
TS , 
CONNID , 
ANI , 
DNIS , 
LAST_VQ , 
TECHNICAL_RESULT , 
TECHNICAL_RESULT_CODE , 
RESULT_REASON , 
RESULT_REASON_CODE , 
DUREE_CONVERSATION ,
PLACE_KEY ,
UD_SITE_CHOISI , 
INTERACTION_TYPE , 
INTERACTION_TYPE_CODE , 
PLACE , 
RESOURCE_TYPE , 
RESOURCE_NAME , 
NOM , 
DUREE_FILE ,
ORIGINAL_FILE_NAME , 
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
CURRENT_TIMESTAMP() INSERT_DATE,
UD_SITE_CIBLE,
TECHNICAL_DESCRIPTOR_KEY,
SEGMENT,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(EVENT_TIME, 'dd/MM/yyyy HH:mm:ss')) )EVENT_DATE ,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) FILE_DATE
FROM CTI.TT_IRF_USER_DATA_CUST C
-- LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.IT_IRF_USER_DATA_CUST WHERE FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.IT_IRF_USER_DATA_CUST )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
