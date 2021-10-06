create table TMP.TT_SCANS_FANTAISISTES_AMELIORE (
    msisdn varchar(50),
    type_personne varchar(50)
) STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')