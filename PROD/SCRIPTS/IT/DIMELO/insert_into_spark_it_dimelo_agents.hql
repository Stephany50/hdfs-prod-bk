insert into CDR.SPARK_IT_DIMELO_AGENTS
select
    CAST(CREATED_AT AS TIMESTAMP) CREATED_AT,
    CAST(updated_at AS TIMESTAMP) updated_at,
    id,
    firstname,
    lastname,
    nickname,
    gender,
    email,
    identities,
    role,
    teams,
    categories,
    signatures,
    foreign_id,
    foreign_jwt_id,
    foreign_saml_id,
    enabled,
    invitation_accepted,
    rc_user_id,
    no_password,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -21, 10),'yyyy_MM_dd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -21, 10),'yyyy_MM_dd'))) FILE_DATE
FROM CDR.SPARK_TT_DIMELO_AGENTS C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_DIMELO_AGENTS WHERE FILE_DATE>DATE_SUB(CURRENT_DATE, 7)) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL