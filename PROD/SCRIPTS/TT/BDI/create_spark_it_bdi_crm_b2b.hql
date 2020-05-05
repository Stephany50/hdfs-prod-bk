CREATE EXTERNAL  TABLE CDR.SPARK_tt_bdi_crm_b2b(
EntityHeadOffice  varchar(255),
BillingAccount  varchar(255),
SocialReason  varchar(255),
BusinessRegister  varchar(255),
RepresentativeDocumentNumber  varchar(255),
DocumentType  varchar(255),
DocumentNumber  varchar(255),
District  varchar(255),
Town  varchar(255),
Street  varchar(255),
Other_adress_info  varchar(255),
Post_Code  varchar(255),
Region_Province  varchar(255),
Nationality  varchar(255),
RepresentativeFullname varchar(255)
)COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/BDI/B2B/'
TBLPROPERTIES ('serialization.null.format'='');