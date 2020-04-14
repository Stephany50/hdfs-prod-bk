CREATE TABLE tmp.tt_omny_sdt_4(
    telephone varchar(250),
    loginvendeur varchar(250)
)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')