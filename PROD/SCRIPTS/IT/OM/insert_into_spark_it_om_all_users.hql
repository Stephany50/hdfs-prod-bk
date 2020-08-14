INSERT INTO CDR.SPARK_IT_OM_ALL_USERS PARTITION (ORIGINAL_FILE_DATE)

SELECT
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    USER_ID,
    PROFILE_ID,
    PARENT_USER_ID,
    PARENT_USER_MSISDN,
    MSISDN,
    USER_NAME_PREFIX,
    USER_FIRST_NAME,
    USER_LAST_NAME,
    USER_SHORT_NAME,
    DOB,
    REGISTERED_ON,
    ADDRESS1,
    ADDRESS2,
    STATE,
    CITY,
    COUNTRY,
    SSN,
    DESIGNATION,
    DIVISION,
    CONTACT_PERSON,
    CONTACT_NO,
    EMPLOYEE_CODE,
    SEX,
    ID_NUMBER,
    E_MAIL,
    WEB_LOGIN,
    ACCOUNT_STATUS,
    CREATION_DATE,
    CREATED_BY,
    CREATED_BY_MSISDN,
    NOMADE_CREATED_BY,
    LEVEL1_APP_DATE,
    LEVEL1_APP_BY,
    LEVEL2_APP_DATE,
    LEVEL2_APP_BY,
    OWNER_ID,
    OWNER_MSISDN,
    USER_DOMAIN_CODE,
    USER_CATEGORY_CODE,
    USER_GRADE_NAME,
    MODIFIED_BY,
    MODIFIED_ON,
    MODIFIED_APPROVED_BY,
    MODIFIED_APPROVED_ON,
    DELETED_ON,
    DEACTIVATION_BY,
    DEPARTMENT,
    REGISTRATION_FORM_NUMBER,
    REMARKS,
    GEOGRAPHICAL_DOMAIN,
    GROUP_ROLE,
    FIRST_TRANSACTION_ON,
    COMPANY_CODE,
    USER_TYPE,
    ACTION_TYPE,
    AGENT_CODE,
    CREATION_TYPE,
    BULK_ID,
    IDENTITY_PROOF_TYPE,
    ADDRESS_PROOF_TYPE,
    PHOTO_PROOF_TYPE,
    ID_TYPE,
    ID_NO,
    ID_ISSUE_PLACE,
    ID_ISSUE_DATE,
    ID_ISSUE_COUNTRY,
    ID_EXPIRY_DATE,
    RESIDENCE_COUNTRY,
    NATIONALITY,
    EMPLOYER_NAME,
    POSTAL_CODE,
    SOUSCRIPTION_TYPE,
    MOBILE_GROUP_ROLE,
    LAST_LOGIN_ON,
    USER_GRADE_CODE,
    PARENT_FIRST_NAME,
    PARENT_LAST_NAME,
    OWNER_FIRST_NAME,
    OWNER_LAST_NAME,
    CURRENT_DATE INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, 10, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OM_ALL_USERS C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_OM_ALL_USERS WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL
