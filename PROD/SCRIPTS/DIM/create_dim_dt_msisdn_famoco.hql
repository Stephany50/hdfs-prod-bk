CREATE EXTERNAL  TABLE DIM.tt_msisdn_famoco
(
OUTIL VARCHAR(100),
REGION_COMMERCIALE VARCHAR(200),
REGION_ADMINISTRATIVE VARCHAR(200),
ZONE_PMO VARCHAR(200),
CANAL VARCHAR(100),
PARTNER VARCHAR(100),
IMEI VARCHAR(100),
MSISDN VARCHAR(40)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/BDI/FAMOCO'
TBLPROPERTIES ('serialization.null.format'='');