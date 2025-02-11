CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_SVI_APPEL (
ID_APPEL       VARCHAR(50),
DATE_DEBUT_OMS    VARCHAR(50),
DATE_DEBUT_SVI   VARCHAR(50),
TYPE_HANGUP    VARCHAR(50),
DATE_HANGUP   VARCHAR(50),
LANGUE   VARCHAR(50),
SEGMENT_CLIENT   VARCHAR(50),
MSISDN   VARCHAR(50),
SERVICE   VARCHAR(50),
ETAT_BASE   VARCHAR(50),
NUMERO_TRANSFERT   VARCHAR(50),
TYPE_APPEL   VARCHAR(50),
ORIGINAL_FILE_NAME   VARCHAR(50),
ORIGINAL_FILE_SIZE       INT,
ORIGINAL_FILE_LINE_COUNT INT,
)
COMMENT 'CTI external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/SVI_APPEL'
TBLPROPERTIES ('serialization.null.format'='')
;