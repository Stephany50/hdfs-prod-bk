CREATE TABLE tmp.tt_omny_sdt_2(
    sender_msisdn varchar(100),
    midatetr date,         
    mtt_trans bigint,      
    nb_transaction bigint, 
    firsttran bigint,      
    dest_tran varchar(100),
    serv_tran varchar(100)
)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')