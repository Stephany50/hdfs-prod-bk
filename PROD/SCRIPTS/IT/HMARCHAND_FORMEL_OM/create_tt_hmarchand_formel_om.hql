CREATE EXTERNAL TABLE CDR.TT_HMARCHAND_FORMEL_OM ( 
ORIGINAL_FILE_NAME VARCHAR(200), 
ORIGINAL_FILE_SIZE INT, 
ORIGINAL_FILE_LINE_COUNT INT, 
UserId bigint, 
Name VARCHAR(200), 
Statut VARCHAR(250), 
Email VARCHAR(250), 
Gender VARCHAR(200), 
Birthday DATE, 
Login VARCHAR(250), 
Password VARCHAR(200), 
Msisdn VARCHAR(250), 
PrefLanguage VARCHAR(200), 
UserUid VARCHAR(250), 
UserCategory VARCHAR(250), 
RoleName VARCHAR(250), 
EnTraitement int, 
Created TIMESTAMP, 
Modified TIMESTAMP, 
TypeAgregateurId bigint 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' 
LOCATION '/PROD/TT/HMARCHAND_FORMEL_OM' 
TBLPROPERTIES ('serialization.null.format'='');