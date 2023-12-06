CREATE EXTERNAL TABLE TT.REF_COMPTE_AFD (
    msisdn varchar(20)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/MARCELE/REF/AFD'
TBLPROPERTIES ('serialization.null.format'='');


CREATE TABLE dim.ref_compte_afd AS SELECT * FROM TT.REF_COMPTE_AFD;

-----------Table BACKUP --------------------

-- dim.ref_compte_afd_backup;