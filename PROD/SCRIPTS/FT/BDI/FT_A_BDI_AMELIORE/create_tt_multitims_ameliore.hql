create TABLE TMP.TT_MULTISIMS_AMELIORE (
    NUMERO_PIECE varchar(255),
    NOM_PRENOM varchar(255),
    MSISDN varchar(255),
    TYPE_PERSONNE varchar(255)
) STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')