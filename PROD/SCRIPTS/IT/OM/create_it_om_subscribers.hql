CREATE TABLE CDR.IT_OM_SUBSCRIBERS (
	USER_ID	VARCHAR(20),
	PROFILE_ID	VARCHAR(20),
	PARENT_USER_ID	VARCHAR(20),
	PARENT_USER_MSISDN	VARCHAR(15),
	MSISDN	VARCHAR(15),
	USER_NAME_PREFIX	VARCHAR(100),
	USER_FIRST_NAME	VARCHAR(80),
	USER_LAST_NAME	VARCHAR(80),
	USER_SHORT_NAME	VARCHAR(15),
	DOB	DATE,
	REGISTERED_ON	TIMESTAMP,
	ADDRESS1	VARCHAR(50),
	ADDRESS2	VARCHAR(50),
	STATE	VARCHAR(30),
	CITY	VARCHAR(30),
	COUNTRY	VARCHAR(20),
	SSN	VARCHAR(15),
	DESIGNATION	VARCHAR(30),
	DIVISION	VARCHAR(20),
	CONTACT_PERSON	VARCHAR(80),
	CONTACT_NO	VARCHAR(50),
	EMPLOYEE_CODE	VARCHAR(12),
	SEX	VARCHAR(10),
	ID_NUMBER	VARCHAR(40),
	E_MAIL	VARCHAR(60),
	WEB_LOGIN	VARCHAR(20),
	ACCOUNT_STATUS	VARCHAR(2),
	CREATION_DATE	TIMESTAMP,
	CREATED_BY	VARCHAR(20),
	CREATED_BY_MSISDN	VARCHAR(15),
	NOMADE_CREATED_BY	VARCHAR(10),
	LEVEL1_APPROVED_ON	TIMESTAMP,
	LEVEL1_APPROVED_BY	VARCHAR(30),
	LEVEL2_APPROVED_ON	TIMESTAMP,
	LEVEL2_APPROVED_BY	VARCHAR(30),
	OWNER_ID	VARCHAR(20),
	OWNER_MSISDN	VARCHAR(15),
	USER_DOMAIN_CODE	VARCHAR(10),
	USER_CATEGORY_CODE	VARCHAR(10),
	USER_GRADE_NAME	VARCHAR(40),
	MODIFIED_BY	VARCHAR(30),
	MODIFIED_ON	TIMESTAMP,
	MODIFIED_APPROVED_BY	VARCHAR(25),
	MODIFIED_APPROVED_ON	TIMESTAMP,
	DELETED_ON	TIMESTAMP,
	DEACTIVATION_BY	VARCHAR(50),
	DEPARTMENT	VARCHAR(20),
	REGISTRATION_FORM_NUMBER	VARCHAR(20),
	REMARKS	VARCHAR(100),
	GEOGRAPHICAL_DOMAIN	VARCHAR(50),
	GROUP_ROLE	VARCHAR(35),
	FIRST_TRANSACTION_ON	DATE,
	COMPANY_CODE	VARCHAR(15),
	USER_TYPE	VARCHAR(10),
	ACTION_TYPE	VARCHAR(20),
	AGENT_CODE	VARCHAR(20),
	CREATION_TYPE	VARCHAR(1),
	BULK_ID	VARCHAR(20),
	IDENTITY_PROOF_TYPE	VARCHAR(60),
	ADDRESS_PROOF_TYPE	VARCHAR(60),
	PHOTO_PROOF_TYPE	VARCHAR(60),
	ID_TYPE	VARCHAR(20),
	ID_NO	VARCHAR(20),
	ID_ISSUE_PLACE	VARCHAR(30),
	ID_ISSUE_DATE	DATE,
	ID_ISSUE_COUNTRY	VARCHAR(40),
	ID_EXPIRY_DATE	DATE,
	RESIDENCE_COUNTRY	VARCHAR(40),
	NATIONALITY	VARCHAR(80),
	EMPLOYER_NAME	VARCHAR(80),
	POSTAL_CODE	VARCHAR(20),
	SOUSCRIPTION_TYPE	VARCHAR(20),
	MOBILE_GROUP_ROLE 	VARCHAR(38),
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
	ORIGINAL_FILE_DATE DATE,
	INSERT_DATE TIMESTAMP
)
PARTITIONED BY (MODIFICATION_DATE DATE)
CLUSTERED BY(USER_ID) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
