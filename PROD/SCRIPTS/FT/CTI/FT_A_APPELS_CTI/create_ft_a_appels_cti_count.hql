CREATE TABLE AGG.FT_A_APPELS_CTI_COUNT  (
mois varchar(10),
semaine varchar(10),
event_date date,
nombre_client int,
type_periode varchar(20),
insert_date timestamp
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;