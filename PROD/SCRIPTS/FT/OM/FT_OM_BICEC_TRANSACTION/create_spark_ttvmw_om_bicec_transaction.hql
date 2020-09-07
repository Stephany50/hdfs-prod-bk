CREATE  TABLE  MON.SPARK_TTVMW_OM_BICEC_TRANS
(
age                         varchar(90  ),                  
   dev                         varchar(90  ),                  
   cha                         varchar(90  ),                  
   ncp                         varchar(90  ),                  
   suf                         varchar(5  ),                   
   ope                         varchar(20  ),                  
   uti                         varchar(5  ),                   
   clc                         varchar(90  ),                  
   dco                         varchar(10  ),                  
   dva                         varchar(10  ),                  
   mon                         decimal(17,2  ),                
   sen                         varchar(90  ),                  
   lib                         varchar(30  ),                  
   pie                         varchar(11  ),                  
   mar                         varchar(5  ),                   
   agsa                        varchar(90  ),                  
   agem                        varchar(90  ),                  
   agde                        varchar(90  ),                  
   devc                        varchar(90  ),                  
   mctv                        decimal(17,2  ),                
   pieo                        varchar(11  ),                  
   user_id                     varchar(50  ),                  
   remark                      varchar(200  ),
   insert_date                 timestamp
  )
PARTITIONED  BY  (EVENT_DATE  DATE  )
 STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'  )