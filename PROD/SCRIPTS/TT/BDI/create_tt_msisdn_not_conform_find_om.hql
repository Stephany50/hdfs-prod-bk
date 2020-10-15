CREATE EXTERNAL TABLE dim.tt_msisdn_not_conform_find_om(
msisdn    varchar(255),
numero_piece    varchar(255),
nom    varchar(255),
date_naissance    varchar(255),
nom_absent    varchar(255),
nom_douteux    varchar(255),
date_naissance_absent    varchar(255),
date_naissance_douteux    varchar(255),
numero_piece_absent    varchar(255),
numero_piece_inf_4    varchar(255),
numero_piece_non_authorise    varchar(255),
numero_piece_egale_msisdn    varchar(255),
numero_piece_a_caract_non_auth    varchar(255),
numero_piece_uniquement_lettre    varchar(255),
om_msisdn    varchar(255),
user_first_name    varchar(255),
user_last_name    varchar(255),
id_number    varchar(255),
birth_date   varchar(255)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/BDI/MSISDN_NOT_CONFORM_FIND_OM/'
TBLPROPERTIES ('serialization.null.format'='');