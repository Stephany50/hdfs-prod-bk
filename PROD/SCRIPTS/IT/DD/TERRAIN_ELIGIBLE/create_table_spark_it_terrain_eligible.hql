# Requete de collecte en BD 
select  Id ,
Firstname ,
Lastname ,
PhoneNumber ,
Address ,
CreatedOn ,
CreatedBy ,
LastModifiedOn ,
LastModifiedBy ,
IsDeleted ,
QuarterId ,
CoverageType ,
CustomerType ,
PlatformName ,
Profession ,
ConfirmCoverageType ,
IdSecond,
Label ,
CityId ,
QuartersCreatedOn ,
QuartersCreatedBy ,
QuartersLastModifiedOn ,
QuartersLastModifiedBy ,
QuartersIsDeleted
from 
(
select Id
, Firstname
, Lastname
, PhoneNumber
, Address
, CreatedOn
, CreatedBy
, LastModifiedOn
, LastModifiedBy
, IsDeleted
, QuarterId
, CoverageType
, CustomerType
, PlatformName
, Profession
, ConfirmCoverageType
from Customers
) Customers 
join 
(
select Id as IdSecond,
Label,
CityId,
CreatedOn as QuartersCreatedOn, 
CreatedBy as QuartersCreatedBy,
LastModifiedOn as QuartersLastModifiedOn,
LastModifiedBy as QuartersLastModifiedBy,
IsDeleted as QuartersIsDeleted
from Quarters
) Quarters
on Customers.QuarterID = Quarters.IdSecond
where date(Customers.CreatedOn)='###SLICE_VALUE###'








CREATE EXTERNAL TABLE TT.SPARK_TT_TERRAIN_ELIGIBLE_TDD
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    Id varchar(50),
    Firstname varchar(50),
    Lastname varchar(50),
    PhoneNumber varchar(50),
    Address varchar(50),
    CreatedOn date,
    CreatedBy varchar(50),
    LastModifiedOn date,
    LastModifiedBy varchar(50),
    IsDeleted varchar(50),
    QuarterId varchar(50),
    CoverageType varchar(50),
    CustomerType varchar(50),
    PlatformName varchar(50),
    Profession varchar(50),
    ConfirmCoverageType varchar(50),
    IdSecond varchar(50),
    Label varchar(50),
    CityId varchar(50),
    QuartersCreatedOn date,
    QuartersCreatedBy varchar(50),
    QuartersLastModifiedOn date,
    QuartersLastModifiedBy varchar(50),
    QuartersIsDeleted varchar(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/TERRAIN_ELIGIBLE'
TBLPROPERTIES ('serialization.null.format'='');


CREATE TABLE CDR.SPARK_IT_TERRAIN_ELIGIBLE_TDD (
    Id varchar(50),
    Firstname varchar(50),
    Lastname varchar(50),
    PhoneNumber varchar(50),
    Address varchar(50),
    CreatedOn date,
    CreatedBy varchar(50),
    LastModifiedOn date,
    LastModifiedBy varchar(50),
    IsDeleted varchar(50),
    QuarterId varchar(50),
    CoverageType varchar(50),
    CustomerType varchar(50),
    PlatformName varchar(50),
    Profession varchar(50),
    ConfirmCoverageType varchar(50),
    IdSecond varchar(50),
    Label varchar(50),
    CityId varchar(50),
    QuartersCreatedOn date,
    QuartersCreatedBy varchar(50),
    QuartersLastModifiedOn date,
    QuartersLastModifiedBy varchar(50),
    QuartersIsDeleted varchar(50),
    original_file_name varchar(50)
    )
COMMENT 'SPARK_IT_TERRAIN_ELIGIBLE_TDD'
PARTITIONED BY (insert_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

--- table tmp pour le deversement au dwh

CREATE TABLE TMP.SPARK_IT_TERRAIN_ELIGIBLE_TDD (
    Id varchar(50),
    Firstname varchar(50),
    Lastname varchar(50),
    PhoneNumber varchar(50),
    Address varchar(50),
    CreatedOn date,
    CreatedBy varchar(50),
    LastModifiedOn date,
    LastModifiedBy varchar(50),
    IsDeleted varchar(50),
    QuarterId varchar(50),
    CoverageType varchar(50),
    CustomerType varchar(50),
    PlatformName varchar(50),
    Profession varchar(50),
    ConfirmCoverageType varchar(50),
    IdSecond varchar(50),
    Label varchar(50),
    CityId varchar(50),
    QuartersCreatedOn date,
    QuartersCreatedBy varchar(50),
    QuartersLastModifiedOn date,
    QuartersLastModifiedBy varchar(50),
    QuartersIsDeleted varchar(50),
    original_file_name varchar(50),
    insert_date DATE
    );



-- creation tmp dwh
CREATE TABLE MON.SQ_TERRAIN_ELIGIBLE (
    Id varchar2(255),
    Firstname varchar2(255),
    Lastname varchar2(255),
    PhoneNumber varchar2(255),
    Address varchar2(255),
    CreatedOn date,
    CreatedBy varchar2(255),
    LastModifiedOn date,
    LastModifiedBy varchar2(255),
    IsDeleted varchar2(255),
    QuarterId varchar2(255),
    CoverageType varchar2(255),
    CustomerType varchar2(255),
    PlatformName varchar2(255),
    Profession varchar2(255),
    ConfirmCoverageType varchar2(255),
    IdSecond varchar2(255),
    Label varchar2(255),
    CityId varchar2(255),
    QuartersCreatedOn date,
    QuartersCreatedBy varchar2(255),
    QuartersLastModifiedOn date,
    QuartersLastModifiedBy varchar2(255),
    QuartersIsDeleted varchar2(255),
    original_file_name varchar2(255),
    insert_date date
);
commit;

DECLARE 
    SAMPLE_TABLE VARCHAR2(200); 
    MIN_DATE_PARTITION VARCHAR2(200); 
    MAX_DATE_PARTITION VARCHAR2(200);  
    KEY_COLUMN_PART_NAME VARCHAR2(200);
    KEY_COLUMN_PART_TYPE VARCHAR2(200);   
    PART_OWNER VARCHAR2(200);  
    PART_TABLE_NAME VARCHAR2(200);  
    PART_PARTITION_NAME VARCHAR2(200);
    PART_TYPE_PERIODE VARCHAR2(200);  
    PART_RETENTION NUMBER;  
    PART_TBS_CIBLE VARCHAR2(200);  
    PART_GARDER_01_DU_MOIS VARCHAR2(200);
    PART_PCT_FREE NUMBER;   
    PART_COMPRESSION VARCHAR2(200);  
    PART_ROTATION_ACTIVE VARCHAR2(200);  
    PART_FORMAT VARCHAR2(200);
BEGIN 
    SAMPLE_TABLE := 'MON.SQ_TERRAIN_ELIGIBLE';
    MIN_DATE_PARTITION := '20221201';
    MAX_DATE_PARTITION := '20241231';
    KEY_COLUMN_PART_NAME := 'INSERT_DATE';
    KEY_COLUMN_PART_TYPE := 'JOUR';
    PART_OWNER := 'MON';
    PART_TABLE_NAME := 'FT_TERRAIN_ELIGIBLE';
    PART_PARTITION_NAME := 'FT_TERRAIN_ELIGIBLE';
    PART_TYPE_PERIODE := 'JOUR';
    PART_RETENTION := 1000;
    PART_TBS_CIBLE :=  'TAB_P_MON_J18_256M';
    PART_GARDER_01_DU_MOIS := 'NON';
    PART_PCT_FREE := 0;
    PART_COMPRESSION := 'COMPRESS';
    PART_ROTATION_ACTIVE := 'OUI';
    PART_FORMAT := 'yyyymmdd';
    MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
    COMMIT; 
END;



sqoop export -jt local --connect "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL = TCP)(HOST = 172.21.75.133)(PORT = 20303)))(CONNECT_DATA=(SERVICE_NAME = OCMDWH)))" --username "mon" --password "Mon123ocm#" --hcatalog-table SPARK_IT_TERRAIN_ELIGIBLE_TDD --hcatalog-database TMP --table MON.FT_TERRAIN_ELIGIBLE





