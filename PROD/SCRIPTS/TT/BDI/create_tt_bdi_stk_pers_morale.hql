CREATE EXTERNAL TABLE CDR.TT_BDI_STK_PERS_MORALE(
MSISDN varchar(48),
RAISON_SOCIALE varchar(255),
CANAL varchar(255),
REGION varchar(255),
NOM_REPRESENTANT_LEGAL varchar(100),
PRENOM_REPRESENTANT_LEGAL   varchar(100),
CNI_REPRESENTANT_LOCAL   varchar(100),
CONTACT_TELEPHONIQUE   varchar(48),
ADRESSE_STRUCTURE   varchar(127),
NUMERO_REGISTRE_COMMERCE   varchar(100)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/BDI/STK/'
TBLPROPERTIES ('serialization.null.format'='');