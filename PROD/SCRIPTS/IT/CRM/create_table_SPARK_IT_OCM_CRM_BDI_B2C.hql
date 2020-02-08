CREATE TABLE CDR.SPARK_IT_OCM_CRM_BDI_B2C (
Activation_Date TIMESTAMP,
Status VARCHAR(25),
Status_Reason VARCHAR(25),
Entity_Head_Office VARCHAR(200),
Billing_Account VARCHAR(50),
Billing_Account_Parent VARCHAR(25),
Social_Reason VARCHAR(25),
Business_Register VARCHAR(25),
Representative_Document_Number VARCHAR(25),
MSISDN VARCHAR(25),
Document_Type VARCHAR(25),
Document_Number VARCHAR(25),
Name VARCHAR(25),
First_Name VARCHAR(25),
Birth_Date TIMESTAMP,
Expiration_Date TIMESTAMP,
Birth_Place VARCHAR(25),
Gender VARCHAR(25),
District VARCHAR(25),
Town VARCHAR(25),
Street VARCHAR(25),
Other_adress_info VARCHAR(25),
Post_Code VARCHAR(25),
Region VARCHAR(25),
Father_Name VARCHAR(25),
Document_Issue_Date TIMESTAMP,
Document_Issue_Place VARCHAR(25),
Tutor_Document_Type VARCHAR(25),
Tutor_Document_Number VARCHAR(25),
Tutor_First_Name VARCHAR(25),
Tutor_Name VARCHAR(25),
Tutor_Birth_Date TIMESTAMP,
Tutor_Profession VARCHAR(25),
Tutor_Gender VARCHAR(25),
Update_Date TIMESTAMP,
ORIGINAL_FILE_NAME VARCHAR(30),
ORIGINAL_FILE_SIZE INTEGER,
ORIGINAL_FILE_LINE_COUNT VARCHAR(30),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(Billing_Account) INTO 64 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');