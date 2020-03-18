CREATE TABLE tmp.tt_omny_sdt_1 (
    event_date date,
    msisdn varchar(15),          
    user_name string,            
    address string, 
    date_inscript date,          
    inscripteur varchar(15),     
    id_number varchar(40),       
    sex varchar(10),
    date_of_birth date,          
    receiver_msisdn varchar(100),
    midatedep date, 
    mtt_depot bigint,            
    nb_depot bigint,
    firstdepot bigint,           
    reg_level string
)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')