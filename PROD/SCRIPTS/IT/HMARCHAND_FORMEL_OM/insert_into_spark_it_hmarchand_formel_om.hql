INSERT INTO CDR.SPARK_IT_HMARCHAND_FORMEL_OM
SELECT
    UserId, 
    Name, 
    Statut, 
    Email, 
    Gender, 
    Birthday, 
    Login, 
    Password, 
    Msisdn, 
    PrefLanguage, 
    UserUid, 
    UserCategory, 
    RoleName, 
    EnTraitement, 
    Created, 
    Modified, 
    TypeAgregateurId,
    ORIGINAL_FILE_NAME, 
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(Substring (ORIGINAL_FILE_NAME, -12,8),'yyyyMMdd'))) original_file_date
FROM CDR.TT_HMARCHAND_FORMEL_OM C
LEFT JOIN 
(
    SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME
    FROM CDR.SPARK_IT_HMARCHAND_FORMEL_OM
) T 
ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL