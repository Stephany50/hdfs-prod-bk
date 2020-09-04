create  external table DIM.TT_REF_CBM(
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(25),
ORIGINAL_FILE_LINE_COUNT VARCHAR(25),
msisdn varchar(120),
multi  varchar(150),
operateur varchar(150),
tenure  varchar(159),
segment varchar(145),
region  varchar(255),
zone  varchar(50)
)
COMMENT 'external tables-TT for dim cbm agrega'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/DIM/REF_CBM'
TBLPROPERTIES ('serialization.null.format'='')

