CREATE TABLE MON.SPARK_OMNY_NEW_ACCOUNT
(
  NAMEPREFIX            VARCHAR(250),
  FIRSTNAME             VARCHAR(250),
  LASTNAME              VARCHAR(250),
  MSISDN                VARCHAR(250),
  IDENTIFICATIONNUMBER  VARCHAR(250),
  FORMNUMBER            VARCHAR(250),
  DATEOFBIRTH           VARCHAR(250),
  GENDER                VARCHAR(250),
  ADRESS                VARCHAR(250),
  DISTRICT              VARCHAR(250),
  CITY                  VARCHAR(250),
  STATE                 VARCHAR(250),
  COUNTRY               VARCHAR(250),
  DESCRIPTION           VARCHAR(250),
  PREFERREDLANGUAGE     VARCHAR(250),
  TYPEOFIDENTITYPROOF   VARCHAR(250),
  MOBILEGROUPROLEID     VARCHAR(250),
  GRADECODE             VARCHAR(250),
  TCPPROFILEID          VARCHAR(250),
  PRIMARYACCOUNT        VARCHAR(250),
  CUSTOMERID            VARCHAR(250),
  ACCOUNTNUMBER         VARCHAR(250),
  ACCOUNTTYPE           VARCHAR(250),
  WALLETTYPE            VARCHAR(250),
  USERSTATUS            VARCHAR(250),
  MIDDLENAME            VARCHAR(250),
  NATIONALITY           VARCHAR(250),
  IDTYPE                VARCHAR(250),
  IDNUMBER              VARCHAR(250),
  PLACEOFIDISSUED       VARCHAR(250),
  ISSUEDCOUNTRYCODE     VARCHAR(250),
  RESIDENCYCOUNTRYCODE  VARCHAR(250),
  ISSUEDDATE            DATE,
  ISIDEXPIRES           VARCHAR(250),
  EXPIREDATE            DATE,
  POSTALCODE            VARCHAR(250),
  EMPLOYERNAME          VARCHAR(250),
  INSERT_DATE           TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')