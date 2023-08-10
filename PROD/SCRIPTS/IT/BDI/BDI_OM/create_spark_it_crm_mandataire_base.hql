CREATE EXTERNAL TABLE CDR.TT_CRM_MANDATAIRE_BASE (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    new_mandatairepartenaireId varchar(250), 
    new_NomPrenom varchar(250), 
    new_Numerodepicedumandataire varchar(250), 
    new_NumerodecompteOMdumandataire varchar(250), 
    new_NumerodelIBUdumandataire varchar(250),
    new_Partenaire varchar(250), 
    new_Datedenaissance varchar(250), 
    new_Nationalite varchar(250),
    new_Typepieceidentite varchar(250), 
    new_Statutdumandataire varchar(250), 
    new_Nom varchar(250), 
    new_Prenom varchar(250), 
    new_Sexe varchar(250), 
    new_Identifiantinternedumandataire varchar(250), 
    new_Qualitdumandataire varchar(250), 
    new_DatedExpirationdelapiece varchar(250), 
    new_DelivrerA varchar(250) 
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/RAW/CRM/BDI_OM/MANDATAIRE'
TBLPROPERTIES ('serialization.null.format'='')
;


CREATE TABLE CDR.SPARK_IT_CRM_MANDATAIRE_BASE
(
    new_mandatairepartenaireId varchar(250), 
    new_NomPrenom varchar(250), 
    new_Numerodepicedumandataire varchar(250), 
    new_NumerodecompteOMdumandataire varchar(250), 
    new_NumerodelIBUdumandataire varchar(250),
    new_Partenaire varchar(250), 
    new_Datedenaissance varchar(250), 
    new_Nationalite varchar(250),
    new_Typepieceidentite varchar(250), 
    new_Statutdumandataire varchar(250), 
    new_Nom varchar(250), 
    new_Prenom varchar(250), 
    new_Sexe varchar(250), 
    new_Identifiantinternedumandataire varchar(250), 
    new_Qualitdumandataire varchar(250), 
    new_DatedExpirationdelapiece varchar(250), 
    new_DelivrerA varchar(250),
    ORIGINAL_FILE_NAME  VARCHAR(50),
    ORIGINAL_FILE_SIZE  INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
   )
  PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");