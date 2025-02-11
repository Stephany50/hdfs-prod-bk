 CREATE TABLE CDR.SPARK_IT_KAABU_CLIENT_DEC
(
 NUMERO_DOSSIER varchar(250), 
 STATUS_ENTREE  varchar(250), 
 STATUS_SORTIE varchar(250), 
 PLATEAU   varchar(250), 
 REMONTEE_DOSSIER   varchar(250), 
 DATE_PREMIER_TRAITEMENT varchar(250), 
 TELEPHONE varchar(250), 
 DATE_DERNIER_TRAITEMENT varchar(250), 
 original_file_name varchar(200) ,
 original_file_size int,
 original_file_line_count  int,
 INSERT_DATE TIMESTAMP

)
  PARTITIONED BY (ORIGINAL_FILE_DATE DATE,DATE_CONTROLE DATE)
  CLUSTERED BY(NUMERO) INTO 64 BUCKETS
  STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


CREATE EXTERNAL TABLE CDR.tt_kaabu_client_directory (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    NUMERO_DOSSIER varchar(250), 
    STATUS_ENTREE  varchar(250), 
    STATUS_SORTIE varchar(250), 
    PLATEAU   varchar(250), 
    REMONTEE_DOSSIER   varchar(250), 
    DATE_PREMIER_TRAITEMENT varchar(250), 
    TELEPHONE varchar(250), 
    DATE_DERNIER_TRAITEMENT varchar(250)
    )
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/IDENTIFICATION/KAABU_DEC'
TBLPROPERTIES ('serialization.null.format'='')
;