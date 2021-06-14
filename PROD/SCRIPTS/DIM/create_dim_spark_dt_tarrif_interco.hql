CREATE TABLE DIM.SPARK_DT_TARRIF_INTERCO
(
    DATE_DEBUT DATE,
    DATE_FIN DATE,
    USAGE VARCHAR(255),
    TYPE_HEURE VARCHAR(255),
    TARIF DECIMAL(17,2)
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')