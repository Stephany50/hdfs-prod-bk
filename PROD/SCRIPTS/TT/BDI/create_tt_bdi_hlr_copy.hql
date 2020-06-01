
CREATE EXTERNAL TABLE CDR.tt_bdi_hlr_copy (

MSISDN  VARCHAR(30),
odbOutgoingCalls  VARCHAR(30),
odbIncomingCalls VARCHAR(30),
ProfileID  VARCHAR(255)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/BDI/HLR_COPY/'
TBLPROPERTIES ('serialization.null.format'='');
