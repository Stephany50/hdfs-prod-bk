CREATE TABLE MON.SPARK_FT_CBM_ARPU_MOU_NEW (
msisdn   VARCHAR(255),
arpu   VARCHAR(255),
arpu_voix   VARCHAR(255),
arpu_onet   VARCHAR(255),
arpu_ofnet   VARCHAR(255),
arpu_inter   VARCHAR(255),
arpu_data   VARCHAR(255),
arpu_VAS   VARCHAR(255),
arpu_roaming_voix   VARCHAR(255),
arpu_roaming   VARCHAR(255),
COMBO_DATA   VARCHAR(255),
combo_voix   VARCHAR(255),
VAS_AMT   VARCHAR(255),
VAS_NB   VARCHAR(255),
PAYG_VOIX   VARCHAR(255),
PAYG_VOIX_onnet VARCHAR(255),
PAYG_VOIX_offnet VARCHAR(255),
PAYG_VOIX_inter VARCHAR(255),
PAYG_VOIX_roaming VARCHAR(255),
MOU_ONNET   VARCHAR(255),
MOU_OFNET   VARCHAR(255),
MOU_INTER   VARCHAR(255),
MOU   VARCHAR(255),
PAYGO_WEBI VARCHAR(255),
SMS_WEBI VARCHAR(255),
ARPU2 VARCHAR(255),
bdles_voix VARCHAR(255),
bdles_onet   VARCHAR(255),
bdles_onnet   VARCHAR(255),
bdles_ofnet   VARCHAR(255),
bdles_inter   VARCHAR(255),
bdles_data   VARCHAR(255),
bdles_roaming_voix   VARCHAR(255),
bdles_roaming_data   VARCHAR(255),
Parrain   VARCHAR(255),
PAYG_DATA   VARCHAR(255),
nb_calls   VARCHAR(255),
REF_AMT   VARCHAR(255),
REF_NB   VARCHAR(255),
INC_NB_CALLS   VARCHAR(255),
volume_data   VARCHAR(255),
volume_chat   VARCHAR(255),
volume_voip   VARCHAR(255),
volume_ott   VARCHAR(255),
ott_user   VARCHAR(255),
region VARCHAR (255),
date_activation DATE,
multi VARCHAR(255),
Segment_valeur VARCHAR(255),
SOURCE VARCHAR(255),
INSERT_DATE TIMESTAMP,
BYTES_DATA DECIMAL(17, 2),
FOU_SMS BIGINT,
VOLUME_4G VARCHAR(255),
VOLUME_3G VARCHAR(255),
VOLUME_2G VARCHAR(255),
PAYGO_SMS VARCHAR(255),
BDLES_SMS VARCHAR(255),
SOS_DATA   VARCHAR(255),
SOS_VOICE   VARCHAR(255),
ARPU_SMS   VARCHAR(255),
DATA_VIA_OM   VARCHAR(255),
VOICE_VIA_OM   VARCHAR(255),
SMS_VIA_OM   VARCHAR(255),
ARPU_DATA_R   VARCHAR(255),
BDLES_VOIX_R   VARCHAR(255),
PAYG_VOIX_R   VARCHAR(255),
ARPU_VOIX_R   VARCHAR(255),
PAYG_SMS_R   VARCHAR(255),
ARPU_SMS_R   VARCHAR(255),
BDLES_INFINITY   VARCHAR(255)
)
COMMENT 'FT_CBM_MOU_ARPU_NEW- FT'
PARTITIONED BY (EVENT_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');