CREATE EXTERNAL TABLE TT.REF_OPERATEURS_CEMAC (
    MCC varchar(20),
    MNC varchar(20),
    OPERATOR_NAME varchar(50),
    COUNTRY_NAME varchar(50),
    SERVICE_FAMILY varchar(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/MARCELE/REF/CEMAC'
TBLPROPERTIES ('serialization.null.format'='');


CREATE TABLE DIM.SPARK_REF_OPERATEURS_CEMAC AS

SELECT * FROM TT.REF_OPERATEURS_CEMAC;
  
CREATE TABLE TMP.SPARK_FT_ROAMING_CEMAC_OUT (
rang  int ,  
msisdn  varchar(50) , 
direction string,
tarrif_plan varchar(50) , 
type_abonne varchar(50) , 
segment_client  varchar(50) , 
pays_visite string,  
code_scr  string , 
tap_code  string , 
operateur_visite varchar(100), 
forfait_roaming  varchar(100), 
subs_channel    string,  
service_type    varchar(50) , 
service_detail string , 
destination string , 
nombre_acte bigint , 
volume_minute  double,  
volume_data bigint , 
revenu_main double,  
event_date  date  
);

CREATE TABLE DIM.SPARK_REF_OPERATEURS_CEMAC_NEW AS
SELECT * FROM TMP.REF_OPERATEURS_CEMAC_NEW;


CREATE EXTERNAL TABLE TMP.REF_OPERATEURS_CEMAC_NEW (
    operateur VARCHAR(200),
    cc VARCHAR(200), 
    ncc  VARCHAR(200), 
    insert_date  VARCHAR(200),  
    length_cc_ncc VARCHAR(200),  
    code_operateur VARCHAR(200),  
    country_name  VARCHAR(200),  
    prefix_trunck VARCHAR(200),  
    length_number VARCHAR(200)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/MARCELE/REF/NEW_CEMAC'
TBLPROPERTIES ('serialization.null.format'='');