
CREATE EXTERNAL TABLE CDR.TT_OM_FLOTTE (
MSISDN  varchar(48),
MSISDN_PARENT   varchar(48)
)COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/BDI/OM_FLOTTE'
TBLPROPERTIES ('serialization.null.format'='');
