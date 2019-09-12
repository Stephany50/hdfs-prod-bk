CREATE TABLE DIM.DT_SMSNOTIFICATION_RECIPIENT (
    msisdn  varchar(12),
    type  varchar(50),
    inserted_date  string,
    full_name  varchar(200),
    direction   varchar(200) ,
    actif     varchar(10)
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");