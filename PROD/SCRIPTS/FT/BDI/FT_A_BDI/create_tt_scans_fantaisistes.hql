create table TMP.TT_SCANS_FANTAISISTES (
    msisdn varchar(50),
    type_personne varchar(50)
) STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')