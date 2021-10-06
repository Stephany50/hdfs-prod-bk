CREATE EXTERNAL TABLE CDR.tt_ref_subscriptions
(
ORIGINAL_FILE_NAME VARCHAR(100),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
bdle_name varchar(500),
prix decimal(17, 2),
coeff_onnet decimal(17, 2),
coeff_offnet decimal(17, 2),
coeff_inter decimal(17, 2),
coeff_data decimal(17, 2),
coef_sms decimal(17, 2),
coef_sva decimal(17, 2),
coeff_roaming decimal(17, 2),
coeff_roaming_voix decimal(17, 2),
coeff_roaming_data decimal(17, 2),
coeff_roaming_sms decimal(17, 2), 
validite bigint,
type_forfait varchar(500),
destination varchar(500),
type_ocm varchar(500),
offre varchar(500),
offer varchar(500),
offer_1 varchar(500),
offer_2 varchar(500),
combo varchar(500)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/REF_SUBSCRIPTIONS'
TBLPROPERTIES ('serialization.null.format'='');